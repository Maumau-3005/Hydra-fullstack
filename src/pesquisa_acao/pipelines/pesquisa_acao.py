# pesquisa_acao.py
# -*- coding: utf-8 -*-
"""
Pipeline robusto (degraus/fallbacks) para:
- Relatórios (CVM + SEC/EDGAR + fallback ADR com XBRL KPIs)
- Notícias (Google News RSS multilíngue) com aliases e expansões via LLM
- Reddit (PRAW -> Reddit RSS) **qualitativo e universal**, com expansão de aliases/subreddits via LLM
- Sumarização via Google AI Studio (Gemini 2.5 Flash) com heurística de fallback
- Multilíngue: pt-BR, en-US, zh-CN (ou o que vier no config)
- Dedup, retries/backoff, cache opcional
- Logs detalhados e _debug para depuração

GRÁTIS — sem dependência de APIs pagas.

Instalação mínima:
  pip install google-genai praw feedparser trafilatura httpx pydantic

Opcionais (recomendado p/ robustez e PDFs):
  pip install tenacity requests-cache url-normalize pymupdf pdfplumber

Credenciais (Colab ou variáveis de ambiente):
  GOOGLE_API_KEY
  REDDIT_CLIENT_ID / REDDIT_CLIENT_SECRET / REDDIT_USER_AGENT   # PRAW (grátis)

Importante p/ SEC:
  export USER_AGENT="seu-app/1.0 (contato: voce@dominio.com; site: https://seusite.com)"

Config externo (opcional):
- Variável PESQ_CONFIG aponta para um JSON. Se não existir, defaults internos serão usados.
Exemplo de config.json:
{
  "langs_news": [
    {"hl":"pt-BR","gl":"BR","ceid":"BR:pt-BR"},
    {"hl":"en-US","gl":"US","ceid":"US:en"},
    {"hl":"zh-CN","gl":"CN","ceid":"CN:zh-Hans"}
  ],
  "gemini_models": ["gemini-2.5-flash","gemini-2.5-flash-lite","gemini-2.5-pro"],
  "default_subreddits": ["stocks","investing","StockMarket","valueinvesting","wallstreetbets"],
  "sector_hints": {
    "Energy":["energy","oil","naturalgas","renewableenergy","utilities"]
  },
  "blocked_domains": ["facebook.com","instagram.com","x.com"],
  "max_workers": 32,
  "http_timeout": 25,
  "max_txt_chars": 2000,
  "news": {"max_per_lang": 18, "max_total_llm": 30},
  "reddit": {"max_items": 220, "min_per_sr": 2}
}
"""

import os, io, re, json, zipfile, urllib.parse, logging, csv, unicodedata, time, random, pathlib
import datetime as dt
from typing import List, Optional, Tuple, Dict, Any, Set
from concurrent.futures import ThreadPoolExecutor, as_completed

# --------- Base libs ---------
import httpx
import feedparser
from trafilatura import extract
from pydantic import BaseModel, Field, validator

# --------- LLM (Gemini) ---------
from google import genai

# --------- Colab secrets (opcional) ---------
try:
    from google.colab import userdata  # type: ignore
except Exception:
    userdata = None

# --------- Reddit PRAW (opcional) ---------
try:
    import praw  # type: ignore
except Exception:
    praw = None

# --------- Opcionais (robustez) ---------
try:
    from tenacity import retry, stop_after_attempt, wait_exponential_jitter, retry_if_exception_type
except Exception:
    retry = None

try:
    from url_normalize import url_normalize
except Exception:
    def url_normalize(u: str) -> str:
        return u

# PDFs
try:
    import fitz  # PyMuPDF
except Exception:
    fitz = None

try:
    import pdfplumber
except Exception:
    pdfplumber = None

# Cache (requests-cache ajuda se você também usa requests em outros trechos)
try:
    import requests_cache
except Exception:
    requests_cache = None

# ============= CONFIG / DEFAULTS =============
def _load_config() -> Dict[str, Any]:
    cfg_path = os.getenv("PESQ_CONFIG")
    if cfg_path and os.path.exists(cfg_path):
        try:
            with open(cfg_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    # fallback: procurar "config.json" no CWD
    if os.path.exists("config.json"):
        try:
            with open("config.json", "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    # default minimal
    return {
        "langs_news": [
            {"hl": "pt-BR", "gl": "BR", "ceid": "BR:pt-BR"},
            {"hl": "en-US", "gl": "US", "ceid": "US:en"},
            {"hl": "zh-CN", "gl": "CN", "ceid": "CN:zh-Hans"},
        ],
        "gemini_models": ["gemini-2.5-flash","gemini-2.5-flash-lite","gemini-2.5-pro"],
        "default_subreddits": [
            "stocks","StockMarket","wallstreetbets","valueinvesting","securityanalysis",
            "investing","financialindependence","options","economy","technology",
            "business","Entrepreneur","news","Futurology"
        ],
        "sector_hints": {
            "Technology": ["tech","hardware","software","programming","cybersecurity"],
            "Energy": ["energy","oil","naturalgas","renewableenergy","utilities"],
            "Financials": ["banking","finance","wallstreetbetsELITE"],
            "Healthcare": ["biotech","medicine","healthcare","pharmacy"],
            "Consumer": ["retail","ecommerce","marketing","advertising"],
            "Industrial": ["manufacturing","supplychain","aviation"],
            "Telecom": ["telecom","5Gtechnology"],
            "Materials": ["mining","chemistry"],
            "Utilities": ["utilities"]
        },
        "blocked_domains": ["facebook.com","instagram.com","x.com"],
        "max_workers": min(32, (os.cpu_count() or 4) * 5),
        "http_timeout": 25,
        "max_txt_chars": 2000,
        "news": {"max_per_lang": 18, "max_total_llm": 30},
        "reddit": {"max_items": 220, "min_per_sr": 2}
    }

CONFIG = _load_config()

# ============= LOGGING =============
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(levelname)s:%(name)s:%(message)s"
)
logger = logging.getLogger("pesquisa_acao")
for _name in ["trafilatura", "trafilatura.core", "trafilatura.downloads"]:
    logging.getLogger(_name).setLevel(logging.ERROR)

# ============= PERFORMANCE / LIMITES =============
MAX_WORKERS = int(os.getenv("THREADS", str(CONFIG.get("max_workers", 32))))
HTTP_TIMEOUT = int(CONFIG.get("http_timeout", 25))
MAX_TXT_CHARS = int(CONFIG.get("max_txt_chars", 2000))

MAX_NEWS_PER_LANG = int(CONFIG.get("news", {}).get("max_per_lang", 18))
MAX_NEWS_TOTAL_FOR_LLM = int(CONFIG.get("news", {}).get("max_total_llm", 30))

MAX_REDDIT_ITEMS = int(CONFIG.get("reddit", {}).get("max_items", 220))
REDDIT_FETCH_PER_SR_MIN = int(CONFIG.get("reddit", {}).get("min_per_sr", 2))

# ============= CONFIG GERAL =============
USER_AGENT = os.getenv("USER_AGENT", "pesquisa-acao/1.4 (contato: voce@example.com)")
LANGS_NEWS = CONFIG.get("langs_news", [])
GEMINI_MODEL_CANDIDATES = CONFIG.get("gemini_models", ["gemini-2.5-flash","gemini-2.5-flash-lite","gemini-2.5-pro"])
DEFAULT_SUBREDDITS = CONFIG.get("default_subreddits", [])
SECTOR_SUBREDDITS = CONFIG.get("sector_hints", {})
BLOCKED_DOMAINS = set(CONFIG.get("blocked_domains", []))

# ============= PEQUENOS SEEDS (mantidos mínimos) =============
# BR → ADR: usado apenas como "seed"; LLM também sugere tickers ADR e o código testa via SEC.
BR_TO_ADR_SEED = {
    "PETR": ["PBR", "PBR.A"],
    "VALE": ["VALE"],
    "BBDC": ["BBD", "BBDO"],
    "ITUB": ["ITUB"],
    "ABEV": ["ABEV"],
    "ELET": ["EBR", "EBR.B"],
    "SUZB": ["SUZ"],
    "BBAS": ["BDORY"],
    "WEGE": ["WEGEY"],
}

# Classes comuns de tickers US (seed mínima; LLM cobre o resto)
US_CLASS_TICKERS_SEED = {
    "GOOG": ["GOOG", "GOOGL"],
    "GOOGL": ["GOOG", "GOOGL"],
    "BRK.B": ["BRK.B", "BRK.A"],
    "BRK.A": ["BRK.A", "BRK.B"],
    "META": ["META", "FB"],
}

# ============= DISK CACHE p/ expansões LLM (evita custo/latência) =============
CACHE_DIR = pathlib.Path(os.getenv("PESQ_CACHE_DIR", pathlib.Path.home() / ".cache" / "pesquisa_acao"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)
EXPAND_CACHE_FILE = CACHE_DIR / "expand_cache.json"

def _load_expand_cache() -> Dict[str, Any]:
    if EXPAND_CACHE_FILE.exists():
        try:
            return json.load(open(EXPAND_CACHE_FILE, "r", encoding="utf-8"))
        except Exception:
            return {}
    return {}

def _save_expand_cache(cache: Dict[str, Any]) -> None:
    try:
        json.dump(cache, open(EXPAND_CACHE_FILE, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    except Exception:
        pass

EXPAND_CACHE = _load_expand_cache()

# ============= SCHEMAS =============
class Item(BaseModel):
    title: str
    url: str
    published_at: Optional[str] = None
    summary: Optional[str] = None
    sentiment: Optional[float] = Field(default=None, ge=-1, le=1)

class BatchLLM(BaseModel):
    items: List[Item]
    summary: str
    note_score: int = Field(ge=0, le=100)
    note_label: str
    rationale: str

class LLMExpand(BaseModel):
    """Resposta do LLM para aliases e subreddits."""
    aliases: List[str] = Field(default_factory=list, description="nomes alternativos, tickers, marcas, produtos")
    translations: Dict[str, List[str]] = Field(default_factory=dict, description="ex.: {'pt': [...], 'en':[...], 'zh':[...]} ")
    subreddits: List[str] = Field(default_factory=list, description="subreddits relevantes (sem prefixo r/)")
    focus_terms: List[str] = Field(default_factory=list, description="termos qualitativos (earnings, lawsuit, strike, hack, ... até 25)")

    @validator("subreddits", pre=True)
    def _clean_sr(cls, v):
        out = []
        for s in v or []:
            s = str(s).strip().lstrip("r/").split("/")[0]
            if s:
                out.append(s)
        return out

# ============= HELPERS =============
def _now_utc_date() -> dt.date:
    return dt.datetime.now(dt.timezone.utc).date()

def _cap_ref_date(ref_date: dt.date) -> dt.date:
    today = _now_utc_date()
    if ref_date > today:
        logger.info(f"[Datas] ref_date {ref_date} > hoje {today}. Usando ref_cap = hoje.")
    return min(ref_date, today)

def _to_date(s: str) -> dt.date:
    s = s.strip()
    m = re.match(r"^\s*(\d{4})[-/](\d{1,2})[-/](\d{1,2})\s*$", s)
    if m:
        y, mm, dd = map(int, m.groups())
        return dt.date(y, mm, dd)
    if s.startswith("date(") and s.endswith(")"):
        nums = [int(x) for x in re.findall(r"\d+", s)]
        return dt.date(nums[0], nums[1], nums[2])
    return dt.date.fromisoformat(s)

def _parse_dt_iso(s: Optional[str]) -> Optional[dt.datetime]:
    if not s: return None
    ss = s.strip().replace("Z", "+00:00")
    try:
        d = dt.datetime.fromisoformat(ss)
        return d if d.tzinfo else d.replace(tzinfo=dt.timezone.utc)
    except Exception:
        try:
            d = dt.date.fromisoformat(ss[:10])
            return dt.datetime.combine(d, dt.time(0,0), tzinfo=dt.timezone.utc)
        except Exception:
            return None

def _within_window(published_at: Optional[str], ref_date: dt.date, lookback_days: int) -> bool:
    if not published_at: return False
    t = _parse_dt_iso(published_at)
    if not t:
        try:
            d = dt.date.fromisoformat(published_at[:10])
            t = dt.datetime.combine(d, dt.time(0,0), tzinfo=dt.timezone.utc)
        except Exception:
            return False
    d = t.date()
    return (d <= ref_date) and (d >= ref_date - dt.timedelta(days=lookback_days))

def _nota_0a10(score_0a100: int) -> int:
    return max(0, min(10, round(score_0a100 / 10)))

def _normalize_name(s: Optional[str]) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^A-Za-z0-9]+", " ", s).strip()
    return re.sub(r"\s+", " ", s).upper()

def _us_ticker_variants(ticker: str) -> List[str]:
    up = ticker.upper()
    if up in US_CLASS_TICKERS_SEED:
        return US_CLASS_TICKERS_SEED[up][:]
    m = re.match(r"^([A-Z]{1,4})[.\-]([A-Z])$", up)
    if m:
        base, cls = m.groups()
        other = "A" if cls == "B" else "B"
        return [up, f"{base}.{other}"]
    return [up]

def _name_matches(norm_row_name: str, aliases: List[str]) -> bool:
    for alias in aliases:
        if alias and (alias in norm_row_name or norm_row_name in alias):
            return True
    return False

def _get_secret(name: str) -> Optional[str]:
    val = None
    if userdata is not None:
        try:
            val = userdata.get(name)
        except Exception:
            val = None
    return val or os.getenv(name)

def _hostname(url: str) -> str:
    try:
        return urllib.parse.urlparse(url).hostname or ""
    except Exception:
        return ""

# ============= LEXICON / HEURÍSTICA =============
def _lexicon() -> Dict[str, set]:
    pos = {
        "alta","subida","otimista","positivo","crescimento","melhora","recorde","ganho","lucro",
        "aumenta","acima","supera","favorável","forte","fortes","expansão","upgrade","compra",
        "up","beat","growth","improves","record","profit","expansion","upgrade","买入","增长","改善"
    }
    neg = {
        "queda","cai","negativo","piora","prejuízo","risco","problema","baixa","reduz","abaixo",
        "perde","alerta","fraqueza","fraude","downgrade","venda","investigação",
        "down","miss","decline","loss","risk","问题","下跌","亏损","降级","卖出"
    }
    return {"pos": pos, "neg": neg}

def _heuristic_sentiment(items: List[Item]) -> Tuple[List[Item], str, int]:
    lex = _lexicon()
    pos, neg = lex["pos"], lex["neg"]
    scores = []
    for it in items:
        text = f"{it.title} {it.summary or ''}".lower()
        p = sum(1 for w in pos if w in text)
        n = sum(1 for w in neg if w in text)
        sc = 0.0
        if p or n:
            sc = (p - n) / max(1, (p + n))
        it.sentiment = sc
        if not it.summary:
            it.summary = it.title
        scores.append(sc)
    agg = sum(scores)/len(scores) if scores else 0.0
    note100 = int(round((agg + 1.0) * 50))
    note10  = _nota_0a10(note100)
    label = "neutral"
    if   agg >= 0.75: label = "strong_buy"
    elif agg >= 0.50: label = "buy"
    elif agg <= -0.75: label = "strong_sell"
    elif agg <= -0.50: label = "sell"
    resumo = f"Heuristic sentiment (pt/en/zh): mean={agg:.2f} → {note10}/10 ({label})."
    return items, resumo, note10

# ============= GEMINI (Google AI Studio) =============
def _genai_client(api_key: Optional[str] = None) -> genai.Client:
    api_key = api_key or _get_secret("GOOGLE_API_KEY") or _get_secret("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("Defina GOOGLE_API_KEY (ou GEMINI_API_KEY). https://aistudio.google.com/app/apikey")
    return genai.Client(api_key=api_key)

def _normalize_items_for_llm(items: List[Item], max_items: int = 18) -> List[Item]:
    seen = set(); out = []
    for it in items:
        key = (url_normalize(it.url) if it.url else "").strip().lower() or it.title.strip().lower()
        if key in seen:
            continue
        seen.add(key)
        if it.summary and len(it.summary) > MAX_TXT_CHARS:
            it.summary = it.summary[:MAX_TXT_CHARS]
        out.append(it)
        if len(out) >= max_items:
            break
    return out

def _genai_resumir_e_notar(items: List[Item], empresa: str, ticker: str, setor: str, fonte: str,
                            debug: Dict[str, Any], kpis: Optional[Dict[str, Tuple[str,float,str]]] = None) -> Tuple[List[Item], str, int]:
    items = _normalize_items_for_llm(items, max_items=18)
    compact = "\n".join([f"- {it.title}\n{(it.summary or '')}" for it in items])

    kpi_txt = ""
    if kpis:
        parts = []
        for k,(date,val,unit) in kpis.items():
            parts.append(f"{k}={val} {unit} ({date})")
        kpi_txt = "KPIs XBRL até a data: " + "; ".join(parts) + ".\n"

    prompt = (
        "Você é analista de equity. Responda em **português do Brasil**.\n"
        "Fontes podem estar em PT/EN/ZH; normalize as conclusões.\n"
        f"Empresa: {empresa} ({ticker}) | Setor: {setor} | Fonte: {fonte}\n"
        + (kpi_txt if kpi_txt else "") +
        "Para cada item, gere UMA frase de síntese e um sentimento em [-1,1]. "
        "Depois gere: (1) resumo agregado (2–4 frases) (2) nota de 0 a 100 e "
        "label (strong_buy|buy|neutral|sell|strong_sell) apenas com base nestes itens e nos KPIs (se houver).\n"
        f"ITENS:\n{compact}"
    )

    last_error = None
    for model in GEMINI_MODEL_CANDIDATES:
        try:
            logger.info(f"[Gemini] Tentando modelo: {model}")
            client = _genai_client()
            response = client.models.generate_content(
                model=model,
                contents=prompt,
                config={
                    "temperature": 0,
                    "response_mime_type": "application/json",
                    "response_schema": BatchLLM,
                },
            )
            parsed = getattr(response, "parsed", None)
            if parsed is None:
                text = (response.text or "").strip()
                parsed = BatchLLM(**(json.loads(text) if text else {}))
            nota = _nota_0a10(parsed.note_score)
            out_items: List[Item] = []
            for i, it in enumerate(items):
                if i < len(parsed.items):
                    it.summary   = parsed.items[i].summary or it.summary
                    it.sentiment = parsed.items[i].sentiment
                out_items.append(it)
            debug["llm"] = {"provider": "gemini", "model": model, "fallback": False}
            return out_items, parsed.summary, nota
        except Exception as e:
            last_error = e
            logger.warning(f"[Gemini] Falhou com {model}: {type(e).__name__} — tentando próximo.")
    logger.warning(f"[Gemini] Todas as tentativas falharam: {type(last_error).__name__ if last_error else 'Erro desconhecido'}. Usando heurística.")
    debug["llm"] = {"provider": "gemini", "model": None, "fallback": True, "error": str(last_error)}
    return _heuristic_sentiment(items)

# ---------- LLM para expansão de aliases e subreddits ----------
def _expand_cache_key(empresa: str, ticker: str, setor: str) -> str:
    return f"{ticker.upper()}||{empresa.strip()}||{setor.strip()}"

def _llm_expand_aliases_and_subreddits(empresa: str, ticker: str, setor: str, debug: Dict[str,Any]) -> LLMExpand:
    """Usa Gemini para gerar aliases, traduções, subreddits e termos. Cacheia em disco (TTL simples 7 dias)."""
    key = _expand_cache_key(empresa, ticker, setor)
    rec = EXPAND_CACHE.get(key)
    if rec and isinstance(rec, dict):
        try:
            ts = float(rec.get("_ts", 0))
            if time.time() - ts < 7*24*3600:
                parsed = LLMExpand(**rec.get("payload", {}))
                debug["llm_expand"] = {"model": rec.get("model","cache"), "fallback": rec.get("fallback", False), "cache": True}
                return parsed
        except Exception:
            pass

    prompt = (
        "Atue como pesquisador de equity. Gere um JSON compacto com campos:\n"
        "aliases: [strings]\n"
        "translations: { 'pt': [..], 'en':[...], 'zh':[...]} (se aplicável)\n"
        "subreddits: [strings]  # sem prefixo r/\n"
        "focus_terms: [strings] # até 25 termos qualitativos úteis em buscas (earnings, guidance, outage, lawsuit, strike, hack, recall, boycott, layoff, churn, price hike, product issue, CEO, CFO, union, subsidy, ban, regulation, sanction, antitrust, class action, short seller, whistleblower, supply chain, shutdown, plant, outage, cybersecurity, data breach, downtime, margin, debt, downgrade, upgrade)\n\n"
        "Regras:\n- Limite aliases a 20, subreddits a 20.\n- Inclua tickers-irmãos (ex.: GOOG/GOOGL; BRK.A/BRK.B) se existir.\n- Para empresa estrangeira com ADR, inclua tickers ADR se conhecidos.\n- Só liste subreddits que existem publicamente e façam sentido.\n- Evite duplicatas; normalize subreddits sem 'r/'.\n"
        f"Empresa: {empresa}\nTicker: {ticker}\nSetor: {setor}\n"
    )
    last_error = None
    for model in GEMINI_MODEL_CANDIDATES:
        try:
            client = _genai_client()
            response = client.models.generate_content(
                model=model,
                contents=prompt,
                config={
                    "temperature": 0.2,
                    "response_mime_type": "application/json",
                    "response_schema": LLMExpand,
                },
            )
            parsed = getattr(response, "parsed", None)
            if parsed is None:
                text = (response.text or "").strip()
                parsed = LLMExpand(**(json.loads(text) if text else {}))
            debug["llm_expand"] = {"model": model, "fallback": False, "cache": False}
            # save cache
            EXPAND_CACHE[key] = {"_ts": time.time(), "model": model, "fallback": False, "payload": parsed.dict()}
            _save_expand_cache(EXPAND_CACHE)
            return parsed
        except Exception as e:
            last_error = e
            logger.debug(f"[LLM expand] falhou {model}: {e}")

    # Fallback heurístico
    aliases: Set[str] = set([empresa.strip(), ticker.strip()] + _us_ticker_variants(ticker))
    corp = [" Inc", " Inc.", " Corporation", " Corp", " Corp.", " Ltd", " Ltd.", " S.A.", " S A", " PLC", " N.V.", ", SA", ", Inc."]
    for c in corp:
        if empresa.endswith(c):
            aliases.add(empresa.replace(c, "").strip())
    base_sr = set(DEFAULT_SUBREDDITS)
    base_sr.update(SECTOR_SUBREDDITS.get(setor, []))
    focus_terms = [
        "earnings","guidance","outage","lawsuit","strike","hack","recall","boycott","layoff",
        "churn","price hike","product issue","CEO","CFO","union","subsidy","ban","regulation",
        "sanction","antitrust","class action","short seller","whistleblower","supply chain","shutdown",
        "plant","cybersecurity","data breach","downtime","margin","debt","downgrade","upgrade"
    ]
    parsed = LLMExpand(
        aliases=list({a for a in aliases if a})[:20],
        translations={},
        subreddits=[s for s in base_sr][:20],
        focus_terms=focus_terms[:25]
    )
    debug["llm_expand"] = {"model": None, "fallback": True, "error": str(last_error), "cache": False}
    return parsed

# ============= Cache/retry HTTP ============
if requests_cache and os.getenv("ENABLE_REQUESTS_CACHE", "0") == "1":
    try:
        requests_cache.install_cache("pesquisa_acao_cache", backend="sqlite", expire_after=6*3600)
        logger.info("[Cache] requests-cache habilitado (6h).")
    except Exception:
        pass

def _retryable():
    if retry:
        return retry(
            reraise=True,
            stop=stop_after_attempt(3),
            wait=wait_exponential_jitter(initial=1, max=8),
            retry=retry_if_exception_type(Exception),
        )
    def deco(fn): return fn
    return deco

@_retryable()
def _http_get(url: str, timeout: int = HTTP_TIMEOUT) -> Optional[str]:
    if any(d in (url.lower()) for d in BLOCKED_DOMAINS):
        return None
    headers = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"}
    with httpx.Client(headers=headers, timeout=timeout, follow_redirects=True) as cli:
        r = cli.get(url)
        if r.status_code != 200 or not r.text:
            raise RuntimeError(f"GET {url} -> {r.status_code}")
        time.sleep(0.07 + random.random()*0.08)  # rate-limit leve
        return r.text

def _fetch_clean_text(url: str) -> str:
    if not url:
        return ""
    try:
        html = _http_get(url)
    except Exception:
        html = None
    if not html:
        return ""
    try:
        txt = extract(html, include_links=False) or ""
    except Exception:
        txt = ""
    return txt.strip()

def _fetch_pdf_text(url: str, limit_chars: int = 10000) -> str:
    if not url:
        return ""
    text = ""
    try:
        if fitz:
            with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=HTTP_TIMEOUT) as cli:
                r = cli.get(url)
                if r.status_code == 200:
                    with fitz.open(stream=r.content, filetype="pdf") as doc:
                        for page in doc:
                            text += page.get_text()
                            if len(text) >= limit_chars:
                                break
                    return text[:limit_chars]
    except Exception:
        pass
    try:
        if pdfplumber:
            with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=HTTP_TIMEOUT) as cli:
                r = cli.get(url)
                if r.status_code == 200:
                    with io.BytesIO(r.content) as f:
                        with pdfplumber.open(f) as doc:
                            for page in doc.pages:
                                text += page.extract_text() or ""
                                if len(text) >= limit_chars:
                                    break
                    return text[:limit_chars]
    except Exception:
        pass
    return ""

def _extract_text_general(url: str) -> str:
    if not url:
        return ""
    ul = url.lower()
    if ul.endswith(".pdf") and (fitz or pdfplumber):
        return _fetch_pdf_text(url, limit_chars=MAX_TXT_CHARS)
    return _fetch_clean_text(url)[:MAX_TXT_CHARS]

# ============= NOTÍCIAS (Google News RSS) =============
def _google_news_rss(query: str, lang: Dict[str,str]) -> str:
    q = urllib.parse.quote_plus(query)
    return f"https://news.google.com/rss/search?q={q}&hl={lang['hl']}&gl={lang['gl']}&ceid={lang['ceid']}"

def _dedup(items: List[Item]) -> List[Item]:
    seen = set(); out = []
    for it in items:
        key = (url_normalize(it.url) if it.url else "").strip().lower() or it.title.strip().lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out

def _enrich_bodies_parallel(items: List[Item]) -> None:
    def _worker(it: Item) -> str:
        return _extract_text_general(it.url)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(_worker, it): it for it in items}
        for fut in as_completed(futs):
            it = futs[fut]
            try:
                body = fut.result() or ""
            except Exception:
                body = ""
            if body:
                it.summary = body

def _build_news_queries(empresa: str, ticker: str, setor: str, expand: LLMExpand) -> List[str]:
    aliases = set([empresa, ticker] + _us_ticker_variants(ticker))
    aliases.update(expand.aliases or [])
    for lang, arr in (expand.translations or {}).items():
        aliases.update(arr or [])
    aliases = {a.strip() for a in aliases if a and len(a.strip()) >= 2}
    focus = (expand.focus_terms or [])[:10]
    queries = []
    big_or = " OR ".join([f"\"{a}\"" if " " in a else a for a in sorted(aliases)])
    queries.append(big_or)
    for t in focus:
        queries.append(f"({big_or}) {t}")
    return queries[:8]

def _collect_news(company: str, ticker: str, setor: str, expand: LLMExpand,
                  ref_cap: dt.date, lookback_days: int) -> List[Item]:
    items: List[Item] = []
    queries = _build_news_queries(company, ticker, setor, expand)
    langs = LANGS_NEWS or [
        {"hl": "pt-BR", "gl": "BR", "ceid": "BR:pt-BR"},
        {"hl": "en-US", "gl": "US", "ceid": "US:en"},
        {"hl": "zh-CN", "gl": "CN", "ceid": "CN:zh-Hans"},
    ]

    for L in langs:
        lang_items: List[Item] = []
        for q in queries:
            feed_url = _google_news_rss(q, L)
            try:
                feed = feedparser.parse(feed_url)
            except Exception:
                continue
            for e in feed.entries[:MAX_NEWS_PER_LANG]:
                link = e.get("link") or e.get("id") or ""
                title = e.get("title") or ""
                pub = None
                if getattr(e, "published_parsed", None):
                    pub = dt.datetime(*e.published_parsed[:6], tzinfo=dt.timezone.utc).isoformat()
                it = Item(title=title, url=link, published_at=pub)
                if _within_window(it.published_at, ref_cap, lookback_days):
                    lang_items.append(it)
        logger.info(f"[Noticias] {L.get('hl','?')}: {len(lang_items)} itens na janela (pré-dedup).")
        items.extend(lang_items)

    items = _dedup(items)[:MAX_NEWS_TOTAL_FOR_LLM]
    return items

def agente_noticias(empresa: str, ticker: str, setor: str,
                    ref_cap: dt.date, lookback_days: int,
                    debug: Dict[str, Any]) -> Tuple[str, int]:
    t0 = time.time()
    expand = _llm_expand_aliases_and_subreddits(empresa, ticker, setor, debug)
    items = _collect_news(empresa, ticker, setor, expand, ref_cap, lookback_days)
    logger.info(f"[Noticias] Total agregados (dedup): {len(items)}")

    if not items:
        debug["noticias"] = {"n_items": 0, "elapsed_s": round(time.time()-t0, 2)}
        return ("Sem notícias relevantes no período.", 5)

    _enrich_bodies_parallel(items)
    out_items, resumo, nota = _genai_resumir_e_notar(items, empresa, ticker, setor, "Notícias", debug)
    debug["noticias"] = {
        "n_items": len(items),
        "elapsed_s": round(time.time()-t0, 2),
        "queries": _build_news_queries(empresa, ticker, setor, expand)
    }
    return resumo, nota

# ============= REDDIT (PRAW -> RSS) =============
def _reddit_client() -> Optional[Any]:
    if praw is None:
        logger.warning("[Reddit] PRAW não instalado. Use: pip install praw")
        return None
    cid = _get_secret("REDDIT_CLIENT_ID")
    csecret = _get_secret("REDDIT_CLIENT_SECRET")
    uagent = _get_secret("REDDIT_USER_AGENT") or USER_AGENT
    if not (cid and csecret and uagent):
        logger.warning("[Reddit] Credenciais ausentes. Set REDDIT_CLIENT_ID/SECRET/USER_AGENT.")
        return None
    try:
        reddit = praw.Reddit(
            client_id=cid, client_secret=csecret, user_agent=uagent, check_for_async=False
        )
        reddit.read_only = True
        return reddit
    except Exception as e:
        logger.warning(f"[Reddit] PRAW init falhou: {e}")
        return None

def _collect_from_reddit_praw(queries: List[str], subreddits_incl: List[str],
                              limit_total: int = MAX_REDDIT_ITEMS) -> List[dict]:
    reddit = _reddit_client()
    if reddit is None:
        return []
    rows = []
    per_sr = max(REDDIT_FETCH_PER_SR_MIN, limit_total // max(1, len(subreddits_incl)))
    for sr in subreddits_incl:
        try:
            sub = reddit.subreddit(sr)
        except Exception:
            continue
        for q in queries[:6]:
            try:
                subs = sub.search(
                    query=q, sort="new", time_filter="all", syntax="lucene", limit=per_sr
                )
                for s in subs:
                    rows.append({
                        "data": {
                            "title": s.title or "",
                            "permalink": getattr(s, "permalink", "") or "",
                            "url": getattr(s, "url", "") or "",
                            "created_utc": int(getattr(s, "created_utc", 0)) or None,
                            "selftext": (getattr(s, "selftext", "") or "")[:MAX_TXT_CHARS],
                            "subreddit": str(getattr(s, "subreddit", "")) or "",
                            "score": int(getattr(s, "score", 0)) or 0
                        }
                    })
            except Exception as e:
                logger.debug(f"[Reddit] Erro r/{sr} q='{q}': {e}")
                continue
    logger.info(f"[Reddit] PRAW coletou {len(rows)} itens.")
    return rows

def _collect_from_reddit_rss(queries: List[str], subreddits_incl: List[str], limit_per_q: int = 80) -> List[dict]:
    out = []
    # busca global
    for q in queries[:6]:
        rss_url = f"https://www.reddit.com/search.rss?q={urllib.parse.quote_plus(q)}&sort=new"
        try:
            feed = feedparser.parse(rss_url)
        except Exception:
            continue
        for e in feed.entries[:limit_per_q]:
            created_iso = None
            if getattr(e, "published_parsed", None):
                created_iso = dt.datetime(*e.published_parsed[:6], tzinfo=dt.timezone.utc).isoformat()
            out.append({
                "data": {
                    "title": e.get("title",""),
                    "permalink": "",
                    "url": e.get("link",""),
                    "created_utc": int(_parse_dt_iso(created_iso).timestamp()) if created_iso else None,
                    "selftext": (e.get("summary") or "")[:MAX_TXT_CHARS],
                    "subreddit": "",
                    "score": 0
                }
            })
    # busca por subreddit específico via RSS
    for sr in subreddits_incl[:12]:
        rss_url = f"https://www.reddit.com/r/{sr}/search.rss?q={urllib.parse.quote_plus(' OR '.join(queries[:3]))}&restrict_sr=1&sort=new"
        try:
            feed = feedparser.parse(rss_url)
        except Exception:
            continue
        for e in feed.entries[:limit_per_q//2]:
            created_iso = None
            if getattr(e, "published_parsed", None):
                created_iso = dt.datetime(*e.published_parsed[:6], tzinfo=dt.timezone.utc).isoformat()
            out.append({
                "data": {
                    "title": e.get("title",""),
                    "permalink": "",
                    "url": e.get("link",""),
                    "created_utc": int(_parse_dt_iso(created_iso).timestamp()) if created_iso else None,
                    "selftext": (e.get("summary") or "")[:MAX_TXT_CHARS],
                    "subreddit": sr,
                    "score": 0
                }
            })
    logger.info(f"[Reddit] RSS coletou {len(out)} itens.")
    return out

def _build_reddit_queries(empresa: str, ticker: str, setor: str, expand: LLMExpand) -> Tuple[List[str], List[str]]:
    aliases: Set[str] = set([empresa, ticker] + _us_ticker_variants(ticker))
    aliases.update([a for a in expand.aliases or [] if a])
    for arr in (expand.translations or {}).values():
        aliases.update(arr or [])
    aliases = {a.strip() for a in aliases if a and len(a.strip()) >= 2}

    terms = expand.focus_terms or []
    core = " OR ".join([f"\"{a}\"" if " " in a else a for a in sorted(aliases)])
    queries = [core]
    for t in terms[:10]:
        queries.append(f"({core}) {t}")

    base_sr = set(DEFAULT_SUBREDDITS)
    base_sr.update(SECTOR_SUBREDDITS.get(setor, []))
    for sr in (expand.subreddits or []):
        if sr: base_sr.add(sr.strip().lstrip("r/"))
    subreddits = [s for s in sorted(base_sr) if re.match(r"^[A-Za-z0-9_][A-Za-z0-9_]*$", s)]

    return queries[:8], subreddits[:30]

def _enrich_reddit_items(items: List[Item]) -> None:
    def _worker(it: Item) -> str:
        if (not it.summary) and it.url and ("reddit.com" not in it.url.lower()):
            return _extract_text_general(it.url)
        return it.summary or ""
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(_worker, it): it for it in items}
        for fut in as_completed(futs):
            it = futs[fut]
            try:
                body = fut.result() or ""
            except Exception:
                body = ""
            if body:
                it.summary = body[:MAX_TXT_CHARS]

def agente_reddit(empresa: str, ticker: str, setor: str,
                  ref_cap: dt.date, lookback_days: int,
                  debug: Dict[str, Any]) -> Tuple[str, int]:
    expand = _llm_expand_aliases_and_subreddits(empresa, ticker, setor, debug)
    queries, subreddits = _build_reddit_queries(empresa, ticker, setor, expand)
    debug["reddit_query_meta"] = {"queries": queries, "subreddits": subreddits}

    rows = _collect_from_reddit_praw(queries, subreddits, limit_total=MAX_REDDIT_ITEMS)
    if not rows:
        logger.warning("[Reddit] Sem PRAW (ou sem credenciais) ou sem resultados. Usando fallback RSS.")
        rows = _collect_from_reddit_rss(queries, subreddits, limit_per_q=90)

    items: List[Item] = []
    for ch in rows:
        d = ch.get("data", {})
        title = d.get("title") or ""
        permalink = d.get("permalink", "")
        link = ("https://www.reddit.com" + permalink) if permalink else (d.get("url", "") or "")
        ts = d.get("created_utc")
        published_at = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).isoformat() if ts else None
        body = d.get("selftext") or ""
        it = Item(title=title, url=link, published_at=published_at, summary=body[:MAX_TXT_CHARS])
        if _within_window(it.published_at, ref_cap, lookback_days):
            items.append(it)

    before = len(items)
    items = _dedup(items)
    logger.info(f"[Reddit] Itens filtrados na janela: {len(items)} (antes dedup: {before})")

    if not items:
        debug["reddit"] = {"n_items": 0}
        return ("Sem menções relevantes no Reddit no período.", 5)

    _enrich_reddit_items(items)
    out_items, resumo, nota = _genai_resumir_e_notar(items, empresa, ticker, setor, "Reddit", debug)
    debug["reddit"] = {"n_items": len(items)}
    return (resumo, nota)

# ============= RELATÓRIOS (SEC/CVM + ADR + XBRL) =============
def _ticker_eh_br(ticker: str) -> bool:
    return bool(re.search(r"\d$", ticker.strip().upper()))

def _sec_get_cik(ticker: str) -> Optional[str]:
    """Obtém CIK via company_tickers.json. Se falhar, tenta variantes de classe."""
    try:
        with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=HTTP_TIMEOUT) as cli:
            r = cli.get("https://www.sec.gov/files/company_tickers.json")
            if r.status_code != 200:
                return None
            data = r.json()
    except Exception:
        return None
    up = ticker.upper()
    for _, rec in data.items():
        if rec.get("ticker", "").upper() == up:
            return f"{int(rec['cik_str']):010d}"
    # tenta variantes de classe (GOOG/GOOGL etc.)
    for var in _us_ticker_variants(up):
        if var == up:
            continue
        for _, rec in data.items():
            if rec.get("ticker", "").upper() == var:
                return f"{int(rec['cik_str']):010d}"
    return None

def _sec_search_filings_efts(ticker: str, form_types: List[str], size: int = 60) -> List[dict]:
    body = {
        "query": { "query_string": { "query": f'ticker:{ticker.upper()} AND ( ' + " OR ".join([f'formType:\"{f}\"' for f in form_types]) + " )" } },
        "from": 0, "size": size,
        "sort": [{ "filedAt": { "order": "desc" }}]
    }
    headers = {"User-Agent": USER_AGENT, "Content-Type": "application/json"}
    try:
        with httpx.Client(headers=headers, timeout=HTTP_TIMEOUT) as cli:
            r = cli.post("https://efts.sec.gov/LATEST/search-index", json=body)
            if r.status_code != 200:
                logger.info(f"[SEC search-index] Falha {r.status_code}")
                return []
            hits = r.json().get("hits", {}).get("hits", [])
            out = []
            for h in hits:
                src = h.get("_source", {})
                out.append({
                    "adsh": src.get("adsh") or src.get("accNo") or "",
                    "filedAt": src.get("filedAt"),
                    "cik": str(src.get("cik") or ""),
                    "formType": src.get("formType"),
                })
            return out
    except Exception:
        return []

def _sec_filing_index(cik: str, accession: str) -> dict:
    acc = accession.replace("-", "")
    url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc}/index.json"
    try:
        with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=HTTP_TIMEOUT) as cli:
            r = cli.get(url)
            if r.status_code != 200:
                return {}
            return r.json()
    except Exception:
        return {}

def _sec_companyfacts(cik: str) -> dict:
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{int(cik):010d}.json"
    try:
        with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=HTTP_TIMEOUT) as cli:
            r = cli.get(url)
            if r.status_code != 200:
                return {}
            return r.json()
    except Exception:
        return {}

def _xbrl_latest_value(series: List[dict], ref_cap: dt.date) -> Optional[Tuple[str,float,str]]:
    best = None; best_dt = None
    for row in series or []:
        d = row.get("end") or row.get("fy")
        if not d:
            continue
        try:
            dd = dt.date.fromisoformat(str(d)[:10])
        except Exception:
            continue
        if dd <= ref_cap and (best is None or dd > best_dt):
            val = row.get("val")
            best = (str(dd), float(val) if val is not None else 0.0); best_dt = dd
    if not best:
        return None
    return (best[0], best[1], "")

def _xbrl_pick_unit(units_dict: Dict[str, List[dict]]) -> Tuple[str,List[dict]]:
    if "USD" in units_dict:
        return "USD", units_dict["USD"]
    for k, v in units_dict.items():
        if isinstance(v, list) and v:
            return k, v
    return "", []

def _sec_kpis(cik: str, ref_cap: dt.date) -> Dict[str, Tuple[str,float,str]]:
    cf = _sec_companyfacts(cik)
    out: Dict[str, Tuple[str,float,str]] = {}
    facts = cf.get("facts", {}).get("us-gaap", {})
    concepts = {
        "Revenue": "Revenues",
        "NetIncome": "NetIncomeLoss",
        "EPS_Diluted": "EarningsPerShareDiluted",
        "OCF": "NetCashProvidedByUsedInOperatingActivities",
    }
    for label, concept in concepts.items():
        try:
            units = facts[concept]["units"]
        except Exception:
            continue
        unit_name, series = _xbrl_pick_unit(units)
        if not series:
            continue
        val = _xbrl_latest_value(series, ref_cap)
        if val:
            out[label] = (val[0], val[1], unit_name)
    return out

def _choose_best_attachment(index_json: dict) -> Optional[str]:
    items = (index_json.get("directory", {}) or {}).get("item", [])
    if not items:
        return None
    def _build_url(name: str) -> str:
        dirurl = index_json.get("directory", {}).get("url", "")
        if dirurl and not dirurl.endswith("/"):
            dirurl += "/"
        return dirurl + name if dirurl else name

    ex99 = [f for f in items if str(f.get("name","")).upper().startswith("EX-99")]
    if ex99:
        return _build_url(ex99[0]["name"])
    htmls = [f for f in items if str(f.get("name","")).lower().endswith((".htm",".html"))]
    if htmls:
        return _build_url(htmls[0]["name"])
    pdfs = [f for f in items if str(f.get("name","")).lower().endswith(".pdf")]
    if pdfs:
        return _build_url(pdfs[0]["name"])
    primaries = [f for f in items if str(f.get("name","")).lower().startswith("primary")]
    if primaries:
        return _build_url(primaries[0]["name"])
    return _build_url(items[0]["name"])

def _sec_collect_best_item(ticker: str, empresa: str, ref_cap: dt.date) -> Optional[Item]:
    form_priority = ["10-Q","10-K","6-K","20-F","8-K"]
    # 1) efts search-index
    hits = _sec_search_filings_efts(ticker, form_priority, size=60)
    if not hits:
        # 1b) tentar variantes de classe
        for var in _us_ticker_variants(ticker):
            if var == ticker:
                continue
            hits = _sec_search_filings_efts(var, form_priority, size=60)
            if hits:
                break
    if not hits:
        # 2) submissions fallback
        cik = _sec_get_cik(ticker)
        if not cik:
            return None
        sub_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        try:
            with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=HTTP_TIMEOUT) as cli:
                r = cli.get(sub_url)
                if r.status_code != 200:
                    return None
                sub = r.json()
        except Exception:
            return None
        forms = sub.get("filings", {}).get("recent", {})
        items: List[Item] = []
        for i, ftype in enumerate(forms.get("form", [])):
            if ftype not in set(form_priority):
                continue
            fdate = forms["filingDate"][i]
            acc = forms["accessionNumber"][i].replace("-", "")
            prim = forms["primaryDocument"][i]
            doc_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc}/{prim}"
            items.append(Item(
                title=f"{ftype} {empresa} ({fdate})",
                url=doc_url,
                published_at=fdate,
                summary=""
            ))
        latest = _pick_latest_leq(items, ref_cap)
        return latest

    def _d(h):
        return _parse_dt_iso(h.get("filedAt") or "") or dt.datetime.min.replace(tzinfo=dt.timezone.utc)
    hits = [h for h in hits if _d(h).date() <= ref_cap]
    if not hits:
        return None
    pri = {f:i for i,f in enumerate(form_priority)}
    hits.sort(key=lambda h: (pri.get(h.get("formType","ZZZ"), 999), _d(h)), reverse=False)
    best_form = hits[0]["formType"]
    cand = [h for h in hits if h["formType"] == best_form]
    cand.sort(key=lambda h: _d(h), reverse=True)
    chosen = cand[0]
    cik = chosen.get("cik") or _sec_get_cik(ticker)
    adsh = chosen.get("adsh","")
    if not cik or not adsh:
        return None

    idx = _sec_filing_index(cik, adsh)
    best_url = _choose_best_attachment(idx)
    filed_date = (chosen.get("filedAt") or "")[:10] or None
    title = f"{best_form} {empresa} ({filed_date or 'N/D'})"
    summary = _extract_text_general(best_url) if best_url else ""
    return Item(title=title, url=best_url or "", published_at=filed_date, summary=summary)

def _cvm_rows_for_year(year: int, kind: str) -> Optional[List[List[str]]]:
    kind = kind.lower()
    url = f"https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/{kind.upper()}/DADOS/{kind}_cia_aberta_{year}.zip"
    try:
        with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=HTTP_TIMEOUT) as cli:
            r = cli.get(url)
            if r.status_code != 200:
                return None
            z = zipfile.ZipFile(io.BytesIO(r.content))
            name = next((n for n in z.namelist() if n.lower().endswith(".csv") and kind in n.lower() and "cia_aberta" in n.lower()), None)
            if not name:
                return None
            raw = z.read(name).decode("utf-8", errors="ignore")
            rows = list(csv.reader(io.StringIO(raw), delimiter=';', quotechar='"'))
        return rows
    except Exception:
        return None

def _find_cols(header: List[str], *alts) -> Optional[int]:
    header = [c.strip().lower() for c in header]
    for a in alts:
        if a in header:
            return header.index(a)
    return None

def _guess_aliases_br(ticker: str, empresa: str) -> List[str]:
    base = re.sub(r"\d+$", "", (ticker or "").upper())
    norm_emp = _normalize_name(empresa)
    aliases = {norm_emp, empresa, ticker.upper()}
    common = {
        "PETR": "PETROLEO BRASILEIRO S A PETROBRAS",
        "VALE": "VALE S A",
        "BBDC": "BANCO BRADESCO S A",
        "ITUB": "ITAU UNIBANCO HOLDING S A",
        "BBAS": "BANCO DO BRASIL S A",
        "ABEV": "AMBEV S A",
        "WEGE": "WEG S A",
        "ELET": "CENTRAIS ELETRICAS BRASILEIRAS S A ELETROBRAS",
        "SUZB": "SUZANO S A",
    }
    if base in common:
        aliases.add(common[base])
    if norm_emp.endswith(" S A"):
        aliases.add(norm_emp.replace(" S A", ""))
    return list(aliases)

def _cvm_collect_items(empresa: str, ticker: str, ref_cap: dt.date) -> List[Item]:
    items: List[Item] = []
    years = {ref_cap.year, ref_cap.year - 1}
    aliases = _guess_aliases_br(ticker, empresa)

    def _collect_one(kind: str, y: int) -> List[Item]:
        rows = _cvm_rows_for_year(y, kind)
        out: List[Item] = []
        if not rows or len(rows) < 2:
            return out
        header = [c.strip().lower() for c in rows[0]]
        i_nome = _find_cols(header, "denom_cia","nome_cia","nome_empresarial")
        i_data = _find_cols(header, "dt_refer","data_refer","dt_referencias")
        i_link = _find_cols(header, "link_doc","link_documento","end_doc")
        if i_nome is None:
            return out
        for cols in rows[1:]:
            row_name = (cols[i_nome] if i_nome < len(cols) else "") or ""
            norm_row = _normalize_name(row_name)
            if not _name_matches(norm_row, aliases):
                continue
            dt_ref = None; display = str(y)
            if i_data is not None and i_data < len(cols) and (cols[i_data] or "").strip():
                s_val = cols[i_data].strip()
                display = s_val
                try:
                    dt_ref = dt.date.fromisoformat(s_val)
                except Exception:
                    try:
                        d,m,a = s_val.split("/")
                        dt_ref = dt.date(int(a), int(m), int(d))
                    except Exception:
                        dt_ref = None
            url_doc = cols[i_link] if (i_link is not None and i_link < len(cols)) else ""
            out.append(Item(
                title=f"{kind.upper()} {row_name} ({display})",
                url=url_doc,
                published_at=dt_ref.isoformat() if dt_ref else None,
                summary=""
            ))
        return out

    with ThreadPoolExecutor(max_workers=min(4, len(years)*2)) as ex:
        jobs = [ex.submit(_collect_one, kind, y) for kind in ("itr", "dfp") for y in sorted(years, reverse=True)]
        for fut in as_completed(jobs):
            items.extend(fut.result() or [])

    logger.info(f"[Relatorios][CVM] '{empresa}': coletados {len(items)} candidatos (ITR+DFP).")
    return items

def _pick_latest_leq(items: List[Item], ref_cap: dt.date) -> Optional[Item]:
    best = None; best_date = None
    for it in items:
        t = _parse_dt_iso(it.published_at)
        if not t:
            continue
        d = t.date()
        if d <= ref_cap and (best is None or d > best_date):
            best = it; best_date = d
    return best

def _extract_tickers_from_llm_aliases(expand: LLMExpand) -> List[str]:
    """Extrai candidatos a tickers US/ADR a partir de aliases do LLM (heurística)."""
    cands = set()
    for a in (expand.aliases or []):
        a = a.strip().upper()
        if re.match(r"^[A-Z]{1,5}([.\-][A-Z])?$", a):
            cands.add(a)
    return list(cands)

def _try_sec_with_adr_fallback(empresa: str, ticker_br: str, ref_cap: dt.date, debug: Dict[str, Any]) -> Optional[Item]:
    # 1) seeds mínimos por mapa
    tried = set()
    base = re.sub(r"\d+$", "", (ticker_br or "").upper())
    for adr in BR_TO_ADR_SEED.get(base, []):
        tried.add(adr)
        logger.info(f"[Relatorios][SEC] Tentando ADR seed: {adr}")
        it = _sec_collect_best_item(adr, empresa, ref_cap)
        if it:
            debug.setdefault("relatorios", {})["adr_fallback"] = {"via":"seed", "ticker": adr}
            return it
    # 2) usar LLM para sugerir tickers (aliases → regex de ticker)
    try:
        expand = _llm_expand_aliases_and_subreddits(empresa, ticker_br, "BR_ADR", debug)
    except Exception:
        expand = LLMExpand()
    llm_cands = [t for t in _extract_tickers_from_llm_aliases(expand) if t not in tried]
    for adr in llm_cands[:6]:
        logger.info(f"[Relatorios][SEC] Tentando ADR via LLM: {adr}")
        it = _sec_collect_best_item(adr, empresa, ref_cap)
        if it:
            debug.setdefault("relatorios", {})["adr_fallback"] = {"via":"llm", "ticker": adr}
            return it
    return None

def agente_relatorios(empresa: str, ticker: str, setor: str,
                      ref_cap: dt.date, debug: Dict[str, Any]) -> Tuple[str, int]:
    t0 = time.time()

    if _ticker_eh_br(ticker):
        candidatos = _cvm_collect_items(empresa, ticker, ref_cap)
        latest = _pick_latest_leq(candidatos, ref_cap)
        if not latest:
            logger.info(f"[Relatorios] CVM vazia para {ticker}. Tentando SEC via ADR...")
            latest = _try_sec_with_adr_fallback(empresa, ticker, ref_cap, debug)

        if not latest:
            debug["relatorios"] = {
                "selected": None,
                "n_candidates": len(candidatos),
                "elapsed_s": round(time.time()-t0, 2)
            }
            return ("Sem relatórios elegíveis no período.", 5)

        if latest.url and not latest.summary:
            latest.summary = _extract_text_general(latest.url)

        debug["relatorios"] = {
            "selected": latest.title,
            "date": latest.published_at,
            "n_candidates": len(candidatos),
            "elapsed_s": round(time.time()-t0, 2)
        }
        out_items, resumo, nota = _genai_resumir_e_notar([latest], empresa, ticker, setor, "Relatórios", debug)
        return resumo, nota

    else:
        best = _sec_collect_best_item(ticker, empresa, ref_cap)
        if not best:
            for var in _us_ticker_variants(ticker):
                if var != ticker:
                    best = _sec_collect_best_item(var, empresa, ref_cap)
                    if best:
                        break
        if not best:
            debug["relatorios"] = {
                "selected": None,
                "n_candidates": 0,
                "elapsed_s": round(time.time()-t0, 2)
            }
            return ("Sem relatórios elegíveis no período.", 5)

        cik = _sec_get_cik(ticker) or _sec_get_cik(_us_ticker_variants(ticker)[0])
        kpis = _sec_kpis(cik, ref_cap) if cik else {}

        debug["relatorios"] = {
            "selected": best.title,
            "date": best.published_at,
            "n_candidates": 1,
            "elapsed_s": round(time.time()-t0, 2),
            "xbrl_kpis": kpis
        }
        out_items, resumo, nota = _genai_resumir_e_notar([best], empresa, ticker, setor, "Relatórios", debug, kpis=kpis)
        return resumo, nota

# ============= FORMATAÇÃO / ORQUESTRAÇÃO =============
def _bloco_fonte(resumo: str, nota: int) -> dict:
    return {"Empresa": {"resumo": resumo, "nota": nota}}

def _analise_para_data(empresa: str, ticker: str, setor: str, data_ref_str: str, lookback_days: int) -> dict:
    ref_date = _to_date(data_ref_str)
    ref_cap  = _cap_ref_date(ref_date)
    debug: Dict[str, Any] = {"ref_date": str(ref_date), "ref_cap": str(ref_cap)}

    resumo_rel, nota_rel = agente_relatorios(empresa, ticker, setor, ref_cap, debug)
    resumo_not, nota_not = agente_noticias (empresa, ticker, setor, ref_cap, lookback_days, debug)
    resumo_red, nota_red = agente_reddit   (empresa, ticker, setor, ref_cap, lookback_days, debug)

    out = {
        "data_de_referencia": data_ref_str,
        "Relatorios": _bloco_fonte(resumo_rel, nota_rel),
        "Noticias":   _bloco_fonte(resumo_not, nota_not),
        "Reddit":     _bloco_fonte(resumo_red, nota_red),
        "_debug":     debug
    }
    return out

def pesquisa_ação(ação: dict, lookback_days: int = 30) -> dict:
    """
    ação = {
        "codigo_da_ação": "PETR3"  # BR com dígito -> CVM; US sem dígito -> SEC
        "Nome_da_empresa": "Petrobras - Petróleo Brasileiro S.A.",
        "Setor_da_empresa": "Energy",
        "Final_do_teste": "2025-06-30",
        "hojé_em_dia": "2025-12-31"
    }
    """
    ticker = ação.get("codigo_da_ação") or ação.get("codigo_da_acao") or ""
    empresa = ação.get("Nome_da_empresa") or ação.get("nome_da_empresa") or ""
    setor   = ação.get("Setor_da_empresa") or ação.get("setor_da_empresa") or ""
    data_final_teste = ação.get("Final_do_teste")
    data_hoje_str    = ação.get("hojé_em_dia") or ação.get("hoje_em_dia")

    if not (ticker and empresa and setor and data_final_teste and data_hoje_str):
        raise ValueError("Entradas faltando: requer 'codigo_da_ação', 'Nome_da_empresa', 'Setor_da_empresa', 'Final_do_teste', 'hojé_em_dia'.")

    print(f">>> Rodando pesquisa_ação com DEBUG ... (threads={MAX_WORKERS})")
    t0 = time.time()
    analise_hoje  = _analise_para_data(empresa, ticker, setor, data_hoje_str, lookback_days)
    analise_final = _analise_para_data(empresa, ticker, setor, data_final_teste, lookback_days)
    elapsed = round(time.time()-t0, 2)
    print(f">>> Concluído em {elapsed}s.")

    saida = {
        "informacoes_da_empresa": {
            "codigo_da_acao": ticker,
            "nome_da_empresa": empresa
        },
        "analise_de_sentimentos": {
            "hoje_em_dia": analise_hoje,
            "Final_do_teste": analise_final
        },
        "_meta": {"elapsed_s": elapsed, "threads": MAX_WORKERS}
    }
    return saida

# ============= EXEMPLO RÁPIDO =============
if __name__ == "__main__":
    exemplo = {
        "codigo_da_ação": "PETR3",
        "Nome_da_empresa": "Petrobras - Petróleo Brasileiro S.A.",
        "Setor_da_empresa": "Energy",
        "Final_do_teste": "2025-06-30",
        "hojé_em_dia": "2025-09-06"
    }
    print(json.dumps(pesquisa_ação(exemplo, lookback_days=30), ensure_ascii=False, indent=2))

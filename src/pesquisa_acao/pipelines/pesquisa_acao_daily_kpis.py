# -*- coding: utf-8 -*-
"""
pesquisa_acao_daily_kpis.py — diário, rápido, KPIs corretas (US-GAAP + IFRS), sem imputação,
sem AutoETS, com lock-to-publication e remoção de colunas vazias.

Principais pontos:
- PREÇO (yfinance; fallbacks: Stooq/Alpha Vantage se quiser).
- KPIs por SEC CompanyFacts (us-gaap + ifrs-full; forms: 10-Q/10-K/6-K/20-F).
- Alinhamento diário "lock-to-publication": a cada dia de pregão, vale o último 'filed' conhecido.
- NENHUMA imputação: não interpolamos nem preenchemos NaNs. Apenas propagação pós-filed.
- Colunas de KPIs 100% vazias são REMOVIDAS (e os respectivos flags também).
- Debug detalhado e sem “spam” de warnings. Cache leve em memória/disk opcional.

Variáveis de ambiente:
- SEC_USER_AGENT (recomendado; ex.: "SeuNome SeuEmail").
- ALPHAVANTAGE_API_KEY (opcional; só se ativar o uso).
- SEC_CACHE_DIR (opcional; caminho para cache JSON da SEC).
"""

from __future__ import annotations
import os, io, json, hashlib, math, warnings, pathlib
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Set
from datetime import datetime, timezone

import pandas as pd
import numpy as np

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# --------- Dependências externas (todas opcionais com fallback controlado)
try:
    import requests
except Exception:  # pragma: no cover
    requests = None

try:
    import yfinance as yf
except Exception:  # pragma: no cover
    yf = None


# ===================== Utilidades de Debug =====================
@dataclass
class DebugLogger:
    enabled: bool = True
    events: List[Dict[str, Any]] = None

    def __post_init__(self):
        if self.events is None:
            self.events = []

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat(timespec='seconds')

    def log(self, msg: str, **kv) -> None:
        if self.enabled:
            print(f"[DEBUG] {msg}")
        self.events.append({"t": self._now(), "msg": msg, **kv})

    def section(self, title: str) -> None:
        if self.enabled:
            print("\n" + "=" * 80)
            print(title)
            print("=" * 80)
        self.events.append({"t": self._now(), "section": title})

    def df(self, name: str, df: pd.DataFrame, head: int = 5) -> None:
        if self.enabled:
            print(f"[DEBUG][DF] {name}: shape={df.shape}, cols={list(df.columns)}")
            with pd.option_context('display.max_columns', None, 'display.width', 180):
                print(df.head(head))
        self.events.append({"t": self._now(),
                            "df": name, "shape": tuple(df.shape), "cols": list(df.columns)})


# ===================== Fontes de PREÇO =====================
def _ensure_utc(dtser: pd.Series) -> pd.Series:
    s = pd.to_datetime(dtser, utc=True)
    # já tz-aware
    return s

def fetch_prices_yfinance(ticker: str, logger: DebugLogger,
                          period: str = "max", interval: str = "1d") -> Optional[pd.DataFrame]:
    if yf is None:
        logger.log("yfinance indisponível.")
        return None
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period=period, interval=interval, auto_adjust=False, actions=True)
        if hist is None or hist.empty:
            logger.log("yfinance vazio.")
            return None
        # preço ajustado (Adj Close) quando existir
        if "Adj Close" in hist.columns:
            ser = hist["Adj Close"].rename("close_adjusted")
        else:
            ser = hist["Close"].rename("close_adjusted")
        out = ser.reset_index().rename(columns={"Date": "date"})
        out["date"] = _ensure_utc(out["date"])
        # Dividends / Stock Splits
        div = hist.get("Dividends", pd.Series(dtype=float)).reset_index().rename(columns={"Date":"date"})
        spl = hist.get("Stock Splits", pd.Series(dtype=float)).reset_index().rename(columns={"Date":"date"})
        if not div.empty:
            div["date"] = _ensure_utc(div["date"])
        if not spl.empty:
            spl["date"] = _ensure_utc(spl["date"])
        out = out.merge(div, on="date", how="left")
        if "Dividends" not in out.columns:
            out["Dividends"] = np.nan
        out = out.merge(spl, on="date", how="left")
        if "Stock Splits" not in out.columns:
            out["Stock Splits"] = np.nan
        out["source"] = "yfinance"
        logger.log(f"yfinance OK: {ticker}, linhas={len(out)}")
        return out[["date","close_adjusted","Dividends","Stock Splits","source"]]
    except Exception as e:  # pragma: no cover
        logger.log(f"Falha yfinance: {e}")
        return None

def fetch_prices_stooq(ticker: str, logger: DebugLogger) -> Optional[pd.DataFrame]:
    if requests is None:
        logger.log("requests indisponível p/ Stooq.")
        return None
    try:
        sym = ticker.lower()
        if not sym.endswith(".us"):
            sym = f"{sym}.us"
        url = f"https://stooq.com/q/d/l/?s={sym}&i=d"
        r = requests.get(url, timeout=30)
        if r.status_code != 200 or not r.text:
            logger.log(f"Stooq sem dados (HTTP {r.status_code}).")
            return None
        df = pd.read_csv(io.StringIO(r.text))
        if df.empty:
            logger.log("Stooq vazio.")
            return None
        df.rename(columns=str.lower, inplace=True)
        out = pd.DataFrame({
            "date": pd.to_datetime(df["date"], utc=True),
            "close_adjusted": df["close"].astype(float),
            "Dividends": np.nan,
            "Stock Splits": np.nan,
            "source": "stooq",
        })
        logger.log(f"Stooq OK: {ticker}, linhas={len(out)}")
        return out
    except Exception as e:
        logger.log(f"Falha Stooq: {e}")
        return None

ALPHAVANTAGE_URL = "https://www.alphavantage.co/query"

def fetch_prices_alphavantage(ticker: str, logger: DebugLogger) -> Optional[pd.DataFrame]:
    if requests is None:
        logger.log("requests indisponível p/ Alpha Vantage.")
        return None
    api_key = os.getenv("ALPHAVANTAGE_API_KEY")
    if not api_key:
        logger.log("Alpha Vantage ignorado (sem API key).")
        return None
    try:
        params = {"function":"TIME_SERIES_DAILY_ADJUSTED","symbol":ticker,"outputsize":"full","apikey":api_key}
        r = requests.get(ALPHAVANTAGE_URL, params=params, timeout=45)
        r.raise_for_status()
        js = r.json()
        key_ts = "Time Series (Daily)"
        if key_ts not in js:
            logger.log(f"Alpha Vantage estrutura inesperada: {list(js.keys())[:5]}")
            return None
        recs = []
        for d, vals in js[key_ts].items():
            recs.append({
                "date": pd.to_datetime(d, utc=True),
                "close_adjusted": float(vals.get("5. adjusted close", np.nan)),
                "Dividends": float(vals.get("7. dividend amount", 0.0) or 0.0),
                "Stock Splits": float(vals.get("8. split coefficient", 0.0) or 0.0),
                "source": "alphavantage"
            })
        df = pd.DataFrame(recs).sort_values("date").reset_index(drop=True)
        logger.log(f"Alpha Vantage OK: {ticker}, linhas={len(df)}")
        return df
    except Exception as e:
        logger.log(f"Falha Alpha Vantage: {e}")
        return None


def stitch_price_sources(dfs: List[pd.DataFrame], logger: DebugLogger,
                         compute_disparity: bool = True) -> pd.DataFrame:
    dfs = [d for d in dfs if d is not None and not d.empty]
    if not dfs:
        raise RuntimeError("Nenhuma fonte de preço retornou dados.")
    cat = pd.concat(dfs, ignore_index=True)
    cat["date"] = pd.to_datetime(cat["date"], utc=True)
    cat = cat.dropna(subset=["date"]).sort_values("date")
    grp = cat.groupby("date", as_index=True)

    med = grp["close_adjusted"].median().rename("close_adjusted")
    nsrc = grp["source"].nunique().rename("n_sources")

    if compute_disparity:
        lo = grp["close_adjusted"].min()
        hi = grp["close_adjusted"].max()
        disparity_bps = ((hi - lo) / med.replace(0, np.nan) * 10000).rename("disparity_bps")
        flag_disparity = (disparity_bps.fillna(0) > 50)  # limiar pedagógico
    else:
        disparity_bps = pd.Series(index=med.index, data=0.0, name="disparity_bps")
        flag_disparity = pd.Series(index=med.index, data=False, name="flag_disparity")

    out = pd.concat([med, nsrc, disparity_bps, flag_disparity], axis=1).reset_index()

    # Dividends / Splits: preferências por fonte
    def pick_pref(col: str) -> pd.Series:
        w = cat.pivot_table(index="date", columns="source", values=col, aggfunc="first")
        for pref in ["yfinance", "alphavantage", "stooq"]:
            if pref in w.columns:
                return w[pref]
        return pd.Series(index=out["date"], dtype=float)

    out = out.merge(pick_pref("Dividends").rename("Dividends"), on="date", how="left")
    out = out.merge(pick_pref("Stock Splits").rename("Stock Splits"), on="date", how="left")

    out = out.sort_values("date").reset_index(drop=True)
    logger.log("Stitching concluído",
               dias=len(out),
               datas=f"{out['date'].min().date()}..{out['date'].max().date()}")
    return out


# ===================== SEC (companyfacts) =====================
SEC_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{CIK}.json"
SEC_TICKERS_JSON = "https://www.sec.gov/files/company_tickers.json"

FORM_OK = {"10-Q","10-K","6-K","20-F"}
UNIT_PREF = ["USD","USD/shares","USD/share","USD per share"]

# Mapa canônico -> lista de candidatos por taxonomia (us-gaap / ifrs-full)
CANON_MAP: Dict[str, Dict[str, List[str]]] = {
    "Revenues": {
        "us-gaap": ["Revenues","RevenueFromContractWithCustomerExcludingAssessedTax","SalesRevenueNet"],
        "ifrs-full": ["Revenue","RevenueFromContractsWithCustomers"]
    },
    "NetIncomeLoss": {
        "us-gaap": ["NetIncomeLoss","ProfitLoss"],
        "ifrs-full": ["ProfitLoss"]
    },
    "OperatingIncomeLoss": {
        "us-gaap": ["OperatingIncomeLoss","OperatingIncomeLossAlternative"],
        "ifrs-full": ["OperatingProfitLoss"]
    },
    "GrossProfit": {
        "us-gaap": ["GrossProfit"],
        "ifrs-full": ["GrossProfit"]
    },
    "EarningsPerShareBasic": {
        "us-gaap": ["EarningsPerShareBasic"],
        "ifrs-full": ["BasicEarningsLossPerShare"]
    },
    "EarningsPerShareDiluted": {
        "us-gaap": ["EarningsPerShareDiluted"],
        "ifrs-full": ["DilutedEarningsLossPerShare"]
    },
    "Assets": {
        "us-gaap": ["Assets"],
        "ifrs-full": ["Assets"]
    },
    "Liabilities": {
        "us-gaap": ["Liabilities"],
        "ifrs-full": ["Liabilities"]
    },
    "StockholdersEquity": {
        "us-gaap": ["StockholdersEquity","StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"],
        "ifrs-full": ["Equity","EquityAttributableToOwnersOfParent"]
    },
    "CashAndCashEquivalentsAtCarryingValue": {
        "us-gaap": ["CashAndCashEquivalentsAtCarryingValue"],
        "ifrs-full": ["CashAndCashEquivalents"]
    },
    "LongTermDebtNoncurrent": {
        "us-gaap": ["LongTermDebtNoncurrent","LongTermBorrowingsNoncurrent"],
        "ifrs-full": ["NoncurrentBorrowings","NoncurrentFinancialLiabilities"]
    },
    "CurrentAssets": {
        "us-gaap": ["AssetsCurrent"],
        "ifrs-full": ["CurrentAssets"]
    },
    "CurrentLiabilities": {
        "us-gaap": ["LiabilitiesCurrent"],
        "ifrs-full": ["CurrentLiabilities"]
    },
    "ResearchAndDevelopmentExpense": {
        "us-gaap": ["ResearchAndDevelopmentExpense"],
        "ifrs-full": ["ResearchAndDevelopmentExpense"]
    },
    "CapitalExpenditures": {
        "us-gaap": ["CapitalExpenditures","PaymentsToAcquirePropertyPlantAndEquipment"],
        "ifrs-full": ["PaymentsToAcquirePropertyPlantAndEquipment"]
    },
}

# simples cache em memória/arquivo para companyfacts
_SEC_CACHE: Dict[str, Dict[str, Any]] = {}

def _sec_headers() -> Dict[str,str]:
    ua = os.getenv("SEC_USER_AGENT", "YourName Contact@example.com")
    return {"User-Agent": ua}

def resolve_cik_from_ticker(ticker: str, logger: DebugLogger, session: Optional["requests.Session"]=None) -> Optional[str]:
    if requests is None:
        logger.log("requests indisponível p/ SEC.")
        return None
    try:
        s = session or requests.Session()
        r = s.get(SEC_TICKERS_JSON, headers=_sec_headers(), timeout=30)
        r.raise_for_status()
        data = r.json()
        t = ticker.upper()
        for _, entry in data.items():
            if entry.get("ticker","").upper() == t:
                cik = str(entry["cik_str"]).zfill(10)
                logger.log(f"CIK resolvido: {ticker}->{cik}")
                return cik
        logger.log(f"CIK não encontrado para {ticker}.")
        return None
    except Exception as e:
        logger.log(f"Falha resolve CIK: {e}")
        return None

def _sec_cache_path(cik: str) -> Optional[pathlib.Path]:
    cdir = os.getenv("SEC_CACHE_DIR")
    if not cdir:
        return None
    try:
        p = pathlib.Path(cdir).expanduser().resolve()
        p.mkdir(parents=True, exist_ok=True)
        return p / f"companyfacts_{cik}.json"
    except Exception:
        return None

def fetch_companyfacts_json(cik: str, logger: DebugLogger, session: Optional["requests.Session"]=None) -> Optional[Dict[str,Any]]:
    if cik in _SEC_CACHE:
        return _SEC_CACHE[cik]
    if requests is None:
        logger.log("requests indisponível p/ SEC.")
        return None
    try:
        # tentativa de cache em disco
        cache_fp = _sec_cache_path(cik)
        if cache_fp and cache_fp.exists():
            try:
                js = json.loads(cache_fp.read_text(encoding="utf-8"))
                _SEC_CACHE[cik] = js
                logger.log(f"SEC companyfacts (cache hit disco): CIK={cik}")
                return js
            except Exception:
                pass

        s = session or requests.Session()
        r = s.get(SEC_FACTS_URL.format(CIK=cik), headers=_sec_headers(), timeout=60)
        r.raise_for_status()
        js = r.json()
        _SEC_CACHE[cik] = js
        if cache_fp:
            try:
                cache_fp.write_text(json.dumps(js), encoding="utf-8")
            except Exception:
                pass
        logger.log(f"SEC companyfacts baixado: CIK={cik}")
        return js
    except Exception as e:
        logger.log(f"Falha SEC companyfacts: {e}")
        return None

def _pick_unit(units: Dict[str, List[Dict[str,Any]]]) -> Optional[str]:
    if not units:
        return None
    for k in UNIT_PREF:
        if k in units:
            return k
    # qualquer um
    return next(iter(units.keys()), None)

def extract_filings_from_facts(js: Dict[str,Any],
                               logger: DebugLogger,
                               canon_map: Dict[str, Dict[str, List[str]]] = CANON_MAP
                               ) -> pd.DataFrame:
    """
    Constrói um DF 'filings' com colunas:
      ['kpi','tax','tag','unit','end','filed','val','form']
    Escolhe, para cada KPI canônica, o MELHOR tag disponível (maior número de pontos).
    """
    if not js:
        return pd.DataFrame(columns=["kpi","tax","tag","unit","end","filed","val","form"])
    facts = js.get("facts", {})
    rows: List[Dict[str,Any]] = []

    def collect_records(tax_name: str, tag_name: str) -> List[Dict[str,Any]]:
        tax = facts.get(tax_name, {})
        node = tax.get(tag_name)
        if not node:
            return []
        units = node.get("units", {})
        unit_key = _pick_unit(units)
        if not unit_key:
            return []
        out = []
        for rec in units.get(unit_key, []):
            form = rec.get("form")
            if form not in FORM_OK:
                continue
            end = pd.to_datetime(rec.get("end"), errors="coerce", utc=True)
            filed = pd.to_datetime(rec.get("filed"), errors="coerce", utc=True)
            val = pd.to_numeric(rec.get("val"), errors="coerce")
            if pd.isna(end) or pd.isna(filed) or pd.isna(val):
                continue
            out.append({"tax": tax_name, "tag": tag_name, "unit": unit_key,
                        "end": end, "filed": filed, "val": float(val), "form": form})
        return out

    # buscar candidatos e escolher o melhor por KPI canônica
    for kpi, m in canon_map.items():
        candidates: List[Dict[str, Any]] = []

        for tax_name, tag_list in m.items():
            for tag in tag_list:
                recs = collect_records(tax_name, tag)
                if recs:
                    for r in recs:
                        r["kpi"] = kpi
                    candidates.extend(recs)

        if not candidates:
            # nenhum tag disponível para esta KPI -> não cria linha alguma
            continue

        # preferir aquele tag/tax com MAIS pontos
        df_cand = pd.DataFrame(candidates)
        # qual (tax,tag,unit) com mais registros?
        grp = df_cand.groupby(["tax","tag","unit"], as_index=False).size().sort_values("size", ascending=False)
        best_tax, best_tag, best_unit = grp.iloc[0][["tax","tag","unit"]]

        df_best = df_cand[(df_cand["tax"]==best_tax)&(df_cand["tag"]==best_tag)&(df_cand["unit"]==best_unit)] \
                    .sort_values(["filed","end"])
        rows.extend(df_best.to_dict("records"))

        logger.log(f"KPI '{kpi}': usando {best_tax}:{best_tag} [{best_unit}], pontos={len(df_best)}")

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["kpi","tax","tag","unit","end","filed","val","form"])
    df = df.sort_values(["kpi","filed","end"]).reset_index(drop=True)
    return df


# ===================== Calendário de pregão & lock-to-publication =====================
def trading_days_from_price(price: pd.DataFrame) -> pd.DatetimeIndex:
    ix = pd.DatetimeIndex(pd.to_datetime(price["date"], utc=True).dropna().unique())
    return ix.sort_values()

def align_to_next_trading_day(dt: pd.Timestamp, trade_days: pd.DatetimeIndex) -> Optional[pd.Timestamp]:
    pos = trade_days.searchsorted(dt)
    if pos < len(trade_days):
        return trade_days[pos]
    return None

def kpis_step_to_trading_days(filings: pd.DataFrame,
                              trade_days: pd.DatetimeIndex,
                              logger: DebugLogger) -> Tuple[pd.DataFrame, Dict[str, Set[pd.Timestamp]]]:
    """
    Para cada KPI:
      - marca 'report days' = primeiro pregão >= filed
      - gera série diária com merge_asof (último filed <= date)
      - não preenche nada antes do primeiro filed (NaN), não interpola.
    Retorna:
      df_daily_step: ['date', <kpi...>]
      report_days_map: dict(kpi -> set(dates))
    """
    out = pd.DataFrame({"date": trade_days})
    report_days: Dict[str, Set[pd.Timestamp]] = {}
    if filings.empty:
        return out, report_days

    for kpi, grp in filings.groupby("kpi"):
        g = grp[["filed","val"]].dropna().sort_values("filed").copy()
        g["filed"] = pd.to_datetime(g["filed"], utc=True)

        # report days (alinhados ao próximo pregão)
        rset: Set[pd.Timestamp] = set()
        for d in g["filed"]:
            nd = align_to_next_trading_day(d, trade_days)
            if nd is not None:
                rset.add(nd)
        report_days[kpi] = rset

        # merge_asof (último filed <= date)
        merged = pd.merge_asof(
            out[["date"]].sort_values("date"),
            g.rename(columns={"filed":"key"}).sort_values("key"),
            left_on="date", right_on="key", direction="backward"
        )
        out[kpi] = merged["val"].values  # sem preenchimento antes do 1º filed -> NaN

    return out, report_days


# ===================== QA simples: saltos de preço =====================
def qc_jumps_flag(df: pd.DataFrame, logger: DebugLogger,
                  col_price: str = "close_adjusted",
                  min_thr_abs: float = 0.6, mad_mult: float = 6.0) -> pd.DataFrame:
    if df.empty or col_price not in df.columns:
        return df
    s = df[col_price].astype(float)
    logret = np.log(s) - np.log(s.shift(1))
    med = np.nanmedian(logret.values)
    mad = np.nanmedian(np.abs(logret.values - med)) * 1.4826
    thr = max(min_thr_abs, mad_mult * (mad if mad > 0 else 1e-6))
    flag = (np.abs(logret) > thr)
    df["flag_jump"] = flag.fillna(False).astype(bool)
    logger.log("QA preço (logret/MAD) aplicado", jumps=int(df["flag_jump"].sum()), thr=float(thr))
    return df


# ===================== JSON builder =====================
def _checksum_bytes(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()

def build_json_daily(entrada: Dict[str,Any],
                     df_prices: pd.DataFrame,
                     df_final: pd.DataFrame,
                     kpis_kept: List[str],
                     logger: DebugLogger) -> Dict[str,Any]:
    ticker = (entrada.get("codigo_da_ação") or entrada.get("ticker") or entrada.get("codigo_da_acao") or "").upper()
    nome = entrada.get("Nome_da_empresa", "")
    dmin = pd.to_datetime(df_final["date"]).min()
    dmax = pd.to_datetime(df_final["date"]).max()

    # checksums
    try:
        b1 = df_prices.to_csv(index=False).encode()
        b2 = df_final.to_csv(index=False).encode()
        c1 = _checksum_bytes(b1); c2 = _checksum_bytes(b2)
    except Exception:
        c1 = c2 = None

    johnson = {
        "meta_input": {
            "timezone": "UTC",
            "trading_calendar": "NYSE",
            "index_type": "trading_days_NYSE",
            "ajuste_preco": "adjusted_close_dividends_splits",
        },
        "empresa": {
            "ticker": ticker,
            "nome": nome,
            "setor": entrada.get("Setor_da_empresa"),
        },
        "alvo": {
            "variavel": "preco_acao",
            "tipo_alvo": "preco",
            "unidade": "USD",
            "frequencia": "diaria",
            "coluna_valor": "close_adjusted",
        },
        "variaveis": {
            "kpis": [{"nome": t, "frequencia": "diaria (lock_to_publication)", "fonte": "SEC companyfacts"} for t in kpis_kept]
        },
        "operacao": {
            "coleta_em": datetime.now(timezone.utc).isoformat(timespec='seconds'),
            "datas": {"min": str(dmin.date()), "max": str(dmax.date())},
            "linhas": int(len(df_final)),
            "colunas": list(map(str, df_final.columns)),
        },
        "qa_flags": {
            "anomalias_jump": int(df_final.get("flag_jump", pd.Series(False)).sum())
        },
        "proveniencia_opcional": {
            "checksum_prices_csv_md5": c1,
            "checksum_dataset_csv_md5": c2,
        }
    }
    return johnson


# ===================== Função principal =====================
def pesquisa_acao_daily_kpis(entrada: Dict[str,Any], *,
                             debug: bool=True,
                             sources: str="yfinance_only",   # "yfinance_only" | "all"
                             use_alphavantage: bool=False,
                             compute_disparity: bool=False,
                             drop_empty_kpi_cols: bool=True,
                             years_back: Optional[int]=None   # None = 'max'
                             ) -> Tuple[Dict[str,Any], pd.DataFrame]:
    """
    Retorna (json, df_diario_completo) — diário, sem imputação, com KPIs corretas (US-GAAP/IFRS).

    Parâmetros chave:
      - sources="yfinance_only" (rápido) | "all" (tenta Stooq e AlphaVantage se ativado).
      - use_alphavantage=True só se tiver ALPHAVANTAGE_API_KEY.
      - compute_disparity=True calcula disparidade entre fontes (se 'all').
      - drop_empty_kpi_cols=True remove KPI 100% NaN (e flags correspondentes).
      - years_back: limite de anos para preço (ex.: 20). None = tudo (default yfinance 'max').
    """
    logger = DebugLogger(enabled=debug)
    logger.section("Início pesquisa_acao_daily_kpis")

    ticker = (entrada.get("codigo_da_ação") or entrada.get("ticker") or entrada.get("codigo_da_acao") or "").upper()
    if not ticker:
        raise ValueError("Entrada precisa de 'codigo_da_ação' ou 'ticker'.")
    logger.log(f"Ticker={ticker}, Empresa={entrada.get('Nome_da_empresa','')}")

    # 1) PREÇO — coleta multi-fonte (conforme configuração)
    logger.section("Coleta de preço — fontes grátis")
    dfs_price: List[pd.DataFrame] = []
    yf_period = "max" if years_back is None else f"{years_back}y"
    yfd = fetch_prices_yfinance(ticker, logger, period=yf_period)
    if yfd is not None:
        dfs_price.append(yfd)

    if sources == "all":
        stq = fetch_prices_stooq(ticker, logger)
        if stq is not None: dfs_price.append(stq)
        if use_alphavantage:
            av = fetch_prices_alphavantage(ticker, logger)
            if av is not None: dfs_price.append(av)

    if not dfs_price:
        raise RuntimeError("Falha ao obter preço de todas as fontes.")
    for i, d in enumerate(dfs_price):
        logger.df(f"preco_src_{i+1}", d, head=3)

    logger.section("Stitching de preço")
    price = stitch_price_sources(dfs_price, logger, compute_disparity=(compute_disparity and len(dfs_price)>1))
    price["ticker"] = ticker
    logger.df("df_prices_stitched", price, head=6)

    trade_days = trading_days_from_price(price)

    # 2) SEC — KPIs (US-GAAP + IFRS) com escolha de melhor tag por KPI canônica
    logger.section("Fundamentos (SEC) lock-to-publication (sem imputação)")
    session = requests.Session() if requests is not None else None
    cik = resolve_cik_from_ticker(ticker, logger, session=session)
    if cik:
        facts_js = fetch_companyfacts_json(cik, logger, session=session)
        filings = extract_filings_from_facts(facts_js, logger, CANON_MAP)
    else:
        filings = pd.DataFrame(columns=["kpi","tax","tag","unit","end","filed","val","form"])
    logger.df("sec_filings (selecionadas)", filings, head=8)

    # 3) KPI diário stepwise (lock-to-publication) + flags report
    df_kpis_daily, report_days = kpis_step_to_trading_days(filings, trade_days, logger)
    logger.df("kpis_daily_step (raw)", df_kpis_daily, head=8)

    # 4) Consolidar com PREÇO
    df = price.merge(df_kpis_daily, on="date", how="left")

    # 5) Flags 'is_report_*' (booleanas) sem NaN
    for kpi, rset in report_days.items():
        mask = df["date"].isin(list(rset))
        df[f"is_report_{kpi}"] = mask.fillna(False).astype(bool)

    # 6) QA simples de preço
    df = qc_jumps_flag(df, logger)

    # 7) Remover KPIs 100% vazias (e suas flags)
    if drop_empty_kpi_cols:
        kpi_cols = [c for c in df.columns if c not in {
            "date","ticker","close_adjusted","Dividends","Stock Splits","n_sources","disparity_bps","flag_disparity","flag_jump"
        } and not c.startswith("is_report_")]
        to_drop: List[str] = []
        kept: List[str] = []
        for c in kpi_cols:
            if df[c].notna().sum() == 0:
                to_drop.append(c)
                flag_col = f"is_report_{c}"
                if flag_col in df.columns:
                    to_drop.append(flag_col)
            else:
                kept.append(c)
        if to_drop:
            df = df.drop(columns=[c for c in to_drop if c in df.columns])
            logger.log(f"KPIs removidas (100% vazias): {sorted(set(to_drop))}")
        logger.log(f"KPIs mantidas: {kept}")
        kpis_kept = kept
    else:
        kpis_kept = [c for c in df.columns if c not in {
            "date","ticker","close_adjusted","Dividends","Stock Splits","n_sources","disparity_bps","flag_disparity","flag_jump"
        } and not c.startswith("is_report_")]

    # 8) Ordenar colunas
    base_cols = ["date","ticker","close_adjusted","Dividends","Stock Splits","n_sources","disparity_bps","flag_disparity","flag_jump"]
    ordered = [c for c in base_cols if c in df.columns] + sorted(kpis_kept) + [f"is_report_{k}" for k in sorted(kpis_kept)]
    df = df[ordered]
    logger.df("df_final (amostra)", df, head=12)

    # 9) JSON
    johnson = build_json_daily(entrada, price, df, kpis_kept, logger)

    # 10) Resumo
    logger.section("Resumo")
    logger.log(f"Período do dataset: {df['date'].min().date()} .. {df['date'].max().date()}")
    logger.log(f"Linhas: {len(df)}, Colunas: {len(df.columns)}")

    return johnson, df


# ---------------- Execução direta (exemplo) ----------------
if __name__ == "__main__":
    exemplo = {
        "codigo_da_ação": "PBR",  # Petrobras ADR
        "Nome_da_empresa": "Petroleo Brasileiro S.A. - Petrobras",
        "Setor_da_empresa": "Energy",
    }
    johnson, df = pesquisa_acao_daily_kpis(
        exemplo,
        debug=True,
        sources="yfinance_only",   # troque para "all" se quiser Stooq/AV
        use_alphavantage=False,
        compute_disparity=False,
        drop_empty_kpi_cols=True,
        years_back=None,           # None = todo histórico do yfinance
    )

    print("\n===== JSON (chaves principais) =====")
    print({k: johnson[k] for k in ["empresa","variaveis","operacao","qa_flags"]})

    print("\n===== PRIMEIRAS 20 LINHAS =====")
    with pd.option_context('display.max_columns', None, 'display.width', 240):
        print(df.head(20))

    # Opcional: salvar saídas em arquivos locais
    df.to_csv("pesquisa_daily_kpis_df.csv", index=False)
    import json as _json
    with open("pesquisa_daily_kpis_johnson.json", "w", encoding="utf-8") as f:
        _json.dump(johnson, f, ensure_ascii=False, indent=2)
    print("Arquivos 'pesquisa_daily_kpis_df.csv' e 'pesquisa_daily_kpis_johnson.json' salvos com sucesso.")

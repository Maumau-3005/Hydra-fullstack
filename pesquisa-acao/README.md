# pesquisa-acao

Inteligência de mercado para ações com **pipeline qualitativo** (relatórios/notícias/Reddit + LLM) e **pipeline quantitativo diário** (preço + KPIs XBRL, lock‑to‑publication). Pensado para funcionar **100% com ferramentas gratuitas**, com caches, fallbacks e execução reprodutível (Docker/CI).

---

## 📌 Sumário

* [Visão geral](#visão-geral)
* [Destaques](#destaques)
* [Arquitetura & Fluxos](#arquitetura--fluxos)
* [Estrutura do repositório](#estrutura-do-repositório)
* [Requisitos](#requisitos)
* [Instalação](#instalação)
* [Configuração](#configuração)

  * [Variáveis de ambiente](#variáveis-de-ambiente)
  * [config.json (opcional)](#configjson-opcional)
* [Uso rápido (Quickstart)](#uso-rápido-quickstart)

  * [1) Cabeçalho da empresa (empresa\_header.py)](#1-cabeçalho-da-empresa-empresa_headerpy)
  * [2) Pesquisa qualitativa (pesquisa\_acao.py)](#2-pesquisa-qualitativa-pesquisa_acopy)
  * [3) Série diária KPI + preço (pesquisa\_acao\_daily\_kpis.py)](#3-série-diária-kpi--preço-pesquisa_acao_daily_kpipy)
* [Entradas e saídas](#entradas-e-saídas)
* [Desempenho, paralelismo e cache](#desempenho-paralelismo-e-cache)
* [Docker](#docker)
* [Testes e CI](#testes-e-ci)
* [Solução de problemas (FAQ rápido)](#solução-de-problemas-faq-rápido)
* [Roadmap (sugestões)](#roadmap-sugestões)
* [Licença](#licença)

---

## Visão geral

O projeto reúne **3 módulos principais**:

1. **`empresa_header.py`** — Dado um **ticker** (B3/US) e **duas datas**, monta um JSON com **nome da empresa** e **setor GICS**. Usa `yfinance`, normaliza GICS (PT/EN), aplica heurísticas para variantes Yahoo (ex.: `PETR3 → PETR3.SA`) e normaliza datas em ISO-8601.
2. **`pipelines/pesquisa_acao.py`** — Pipeline **qualitativo** que agrega **Relatórios (CVM/SEC)**, **Notícias (Google News RSS)** e **Reddit (PRAW/RSS)**. Faz **sumarização + sentimento** com **Gemini** (fallback heurístico), suporta **pt/en/zh**, dedup, retries, caches e **ADR fallback**.
3. **`pipelines/pesquisa_acao_daily_kpis.py`** — Pipeline **quantitativo diário** que cruza **preço ajustado** (yfinance, com opcionais Stooq/AlphaVantage) com **KPIs XBRL (SEC CompanyFacts)**, alinhados por **lock‑to‑publication** (sem imputação; série stepwise). Exporta **CSV** + **JSON "johnson"** com metadados e QA.

---

## Destaques

* **Grátis e robusto**: usa feeds públicos + fallbacks (RSS/heurística) e evita dependências pagas.
* **Multilíngue** (pt-BR, en-US, zh-CN) com expansão de aliases/subreddits via LLM.
* **Lock-to-publication**: valores de KPIs só passam a valer **após** a data de **filing** (próximo pregão).
* **Sem imputação**: nada de interpolar; apenas propagação pós-filing.
* **Qualidade**: flags de **saltos de preço** (logret/MAD) e remoção automática de KPIs 100% vazias.
* **Modular** (`src/…/core/*`): fácil de testar, trocar fontes e reaproveitar componentes.

---

## Arquitetura & Fluxos

```
[Ticker + Datas] ──► empresa_header.py ──► {nome,setor,final,hoje}
                                       │
                                       ├─► pipelines/pesquisa_acao.py (Qualitativo)
                                       │    ├─ Relatórios: CVM/SEC (XBRL help, ADR fallback)
                                       │    ├─ Notícias: Google News RSS (multi-idiomas)
                                       │    ├─ Reddit: PRAW → RSS (expansão via LLM)
                                       │    └─ LLM: Gemini (sumário + sentimento; fallback heuristic)
                                       │
                                       └─► pipelines/pesquisa_acao_daily_kpis.py (Quantitativo diário)
                                            ├─ Preço: yfinance (opcionais Stooq/AlphaVantage)
                                            ├─ KPIs: SEC CompanyFacts (US-GAAP/IFRS)
                                            ├─ Lock-to-publication (merge_asof)
                                            └─ CSV + JSON (johnson) + QA/flags
```

---

## Estrutura do repositório

```
pesquisa-acao/
├─ README.md
├─ LICENSE
├─ CHANGELOG.md
├─ .gitignore
├─ .editorconfig
├─ .pre-commit-config.yaml
├─ .env.example                 # modelos de credenciais (NÃO commitar .env real)
├─ pyproject.toml               # pacote Python (build, deps, entry points)
├─ config.json                  # exemplo de config (langs, limites, etc.)
├─ Dockerfile                   # ambiente reprodutível
├─ .github/
│  └─ workflows/
│     └─ python-ci.yml          # lint + testes + build
├─ data/
│  ├─ raw/
│  └─ outputs/
├─ examples/
│  ├─ exemplo_input_header.json
│  ├─ exemplo_input_pesquisa.json
│  └─ run_examples.md
├─ scripts/                     # invocadores de linha de comando (VS Code friendly)
│  ├─ header-cli.py             # chama montar_json_empresa
│  ├─ pesquisa-cli.py           # chama pesquisa_ação
│  └─ daily-cli.py              # chama pesquisa_acao_daily_kpis
├─ src/
│  └─ pesquisa_acao/
│     ├─ __init__.py
│     ├─ empresa_header.py
│     ├─ pipelines/
│     │  ├─ pesquisa_acao.py
│     │  └─ pesquisa_acao_daily_kpis.py
│     ├─ core/
│     │  ├─ gics.py, yahoo.py, dates.py, news.py, reddit.py, sec.py, cvm.py,
│     │  │  text_extract.py, llm.py, utils.py
│     └─ config.py
├─ tests/
│  ├─ test_gics.py
│  ├─ test_yahoo_variants.py
│  ├─ test_dates.py
│  ├─ test_xbrl_companyfacts.py
│  └─ test_end_to_end.py
└─ notebooks/
   └─ 01_quali_demo.ipynb
```

---

## Requisitos

* **Python** ≥ 3.10
* **Sistema**: Linux/Mac/Windows.
* **Opcional (PDF/robustez)**: `pymupdf` (PyMuPDF) e `pdfplumber` para extrair texto de PDFs; `requests-cache` para cache de HTTP.

---

## Instalação

Crie um ambiente virtual e instale o pacote (modo editável):

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\\Scripts\\activate
pip install --upgrade pip
pip install -e .
# opcionais recomendados (PDF/robustez)
pip install pymupdf pdfplumber tenacity requests-cache url-normalize
```

Instale hooks locais (lint/format/test rápidos):

```bash
pre-commit install
pre-commit run --all-files
```

---

## Configuração

Crie um arquivo `.env` (baseado em `.env.example`) ou exporte variáveis no shell.

### Variáveis de ambiente

* **LLM (Gemini)**

  * `GOOGLE_API_KEY` *(ou)* `GEMINI_API_KEY`
* **Reddit (opcional, PRAW)**

  * `REDDIT_CLIENT_ID`, `REDDIT_CLIENT_SECRET`, `REDDIT_USER_AGENT`
* **SEC (recomendado)**

  * `USER_AGENT` *(para efts/submissions)*
  * `SEC_USER_AGENT` *(para companyfacts)* → ex.: `"SeuNome seuemail@dominio.com"`
  * `SEC_CACHE_DIR` *(opcional)* → cache no disco para companyfacts
* **AlphaVantage (opcional)**

  * `ALPHAVANTAGE_API_KEY`
* **Geral**

  * `THREADS` (máx. de workers, default dependente de CPU)
  * `PESQ_CONFIG` (caminho para `config.json` customizado)
  * `PESQ_CACHE_DIR` (cache em disco para expansões LLM)
  * `ENABLE_REQUESTS_CACHE=1` (habilita `requests-cache` se instalado)

### `config.json` (opcional)

Exemplo mínimo:

```json
{
  "langs_news": [
    {"hl": "pt-BR", "gl": "BR", "ceid": "BR:pt-BR"},
    {"hl": "en-US", "gl": "US", "ceid": "US:en"}
  ],
  "gemini_models": ["gemini-2.5-flash","gemini-2.5-flash-lite","gemini-2.5-pro"],
  "default_subreddits": ["stocks","investing","StockMarket"],
  "sector_hints": {"Energy": ["energy","oil","naturalgas","renewableenergy","utilities"]},
  "blocked_domains": ["facebook.com","instagram.com","x.com"],
  "max_workers": 32,
  "http_timeout": 25,
  "max_txt_chars": 2000,
  "news": {"max_per_lang": 18, "max_total_llm": 30},
  "reddit": {"max_items": 220, "min_per_sr": 2}
}
```

---

## Uso rápido (Quickstart)

> Os três entrypoints podem ser chamados **via Python** (importando do pacote) ou via **scripts** em `scripts/`. Use `--help` nos scripts para ver flags disponíveis.

### 1) Cabeçalho da empresa (`empresa_header.py`)

**Python**:

```python
from pesquisa_acao.empresa_header import montar_json_empresa
out = montar_json_empresa("PETR3", "2025-06-30", "2025-09-06", debug=True)
print(out)
```

**CLI**:

```bash
python scripts/header-cli.py --help
```

### 2) Pesquisa qualitativa (`pesquisa_acao.py`)

**Python**:

```python
from pesquisa_acao.pipelines.pesquisa_acao import pesquisa_ação
acao = {
  "codigo_da_ação": "PETR3",
  "Nome_da_empresa": "Petrobras - Petróleo Brasileiro S.A.",
  "Setor_da_empresa": "Energy",
  "Final_do_teste": "2025-06-30",
  "hojé_em_dia": "2025-09-06"
}
res = pesquisa_ação(acao, lookback_days=30)
```

**CLI**:

```bash
python scripts/pesquisa-cli.py --help
```

### 3) Série diária KPI + preço (`pesquisa_acao_daily_kpis.py`)

**Python**:

```python
from pesquisa_acao.pipelines.pesquisa_acao_daily_kpis import pesquisa_acao_daily_kpis
entrada = {
  "codigo_da_ação": "PBR",
  "Nome_da_empresa": "Petroleo Brasileiro S.A. - Petrobras",
  "Setor_da_empresa": "Energy"
}
johnson, df = pesquisa_acao_daily_kpis(
  entrada,
  debug=True,
  sources="yfinance_only",   # ou "all" p/ tentar Stooq/AlphaVantage
  use_alphavantage=False,
  compute_disparity=False,
  drop_empty_kpi_cols=True,
  years_back=None
)
# Salvar
df.to_csv("data/outputs/pesquisa_daily_kpis_df.csv", index=False)
import json; json.dump(johnson, open("data/outputs/pesquisa_daily_kpis.json","w",encoding="utf-8"), indent=2, ensure_ascii=False)
```

**CLI**:

```bash
python scripts/daily-cli.py --help
```

---

## Entradas e saídas

### `empresa_header.py`

**Entrada mínima**: ticker + datas (string em vários formatos)
**Saída** (exemplo):

```json
{
  "codigo_da_ação": "PETR3",
  "Nome_da_empresa": "Petrobras - Petróleo Brasileiro S.A.",
  "Setor_da_empresa": "Energy",
  "Final_do_teste": "2025-06-30",
  "hojé_em_dia": "2025-09-06"
}
```

### `pesquisa_acao.py` (qualitativo)

**Saída**: dicionário com blocos `Relatorios`, `Noticias`, `Reddit`, cada um com `resumo` (PT-BR) e `nota` (0–10), além de `_debug` com metadados (queries/latência/modelo LLM).

### `pesquisa_acao_daily_kpis.py` (quantitativo)

* **CSV** diário com colunas: `date`, `ticker`, `close_adjusted`, `Dividends`, `Stock Splits`, (KPIs …), `is_report_*`, flags `flag_jump`, e opcionalmente `n_sources`/`disparity_bps`.
* **JSON "johnson"** com metadados (período, variáveis, QA, checksums) — pronto para versionamento/integração.

> **Lock‑to‑publication:** cada KPI só aparece (ou muda de valor) **a partir** do **primeiro pregão ≥ data de filing**. Antes disso, fica `NaN`.

---

## Desempenho, paralelismo e cache

* Ajuste o paralelismo com `THREADS` ou `config.json.max_workers`.
* Para reduzir latência em HTTP, habilite cache (se instalado): `ENABLE_REQUESTS_CACHE=1`.
* Para SEC CompanyFacts, use `SEC_CACHE_DIR` para reuso entre execuções.
* Algumas rotinas fazem `time.sleep` leve para evitar rate limits.

---

## Docker

Build:

```bash
docker build -t pesquisa-acao:latest .
```

Execução (montando outputs e .env):

```bash
docker run --rm -it \
  --env-file .env \
  -v "$PWD/data/outputs":/app/data/outputs \
  pesquisa-acao:latest \
  python scripts/pesquisa-cli.py --help
```

---

## Testes e CI

* **Testes locais**:

```bash
pytest -q
```

* **CI (GitHub Actions)**: workflow `python-ci.yml` roda lint + testes + build a cada push/PR.
* **pre-commit**: auto-format/checks em cada commit.

---

## Solução de problemas (FAQ rápido)

* **SEC 403 / bloqueio**: defina um `USER_AGENT`/`SEC_USER_AGENT` válido. Evite cargas agressivas.
* **yfinance vazio**: confirme ticker e/ou variantes (ex.: `PETR3.SA`). Rede corporativa pode filtrar.
* **PRAW sem credenciais**: o pipeline **cai para RSS** automaticamente.
* **PDFs sem texto**: instale `pymupdf`/`pdfplumber` (opcionais).
* **LLM indisponível**: a sumarização/sentimento usa **fallback heurístico**.

---

## Roadmap (sugestões)

* Adicionar **exportadores** (Parquet/Feather) e **schemas** p/ validação.
* Métricas de qualidade por fonte (coverage por período/idioma).
* Suporte nativo a **proxies** e **retrys** configuráveis por fonte.
* Enriquecer KPIs (IFRS‑Full vs US‑GAAP) e normalização por moeda.

---

## Licença

Consulte **LICENSE**.

---

> *Dúvidas, bugs ou melhorias? Abra uma issue ou envie um PR!*

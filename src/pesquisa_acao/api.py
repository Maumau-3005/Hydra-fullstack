from fastapi import FastAPI
from pydantic import BaseModel
from typing import Dict, Any
from .empresa_header import montar_json_empresa
from .pipelines.pesquisa_acao import pesquisa_ação
from .pipelines.pesquisa_acao_daily_kpis import pesquisa_acao_daily_kpis

app = FastAPI(title="pesquisa-acao API")


class HeaderIn(BaseModel):
    ticker: str
    final_do_teste: str
    hoje_em_dia: str
    debug: bool = False


@app.post("/header")
def header(inp: HeaderIn):
    return montar_json_empresa(inp.ticker, inp.final_do_teste, inp.hoje_em_dia, debug=inp.debug)


@app.post("/qualitativo")
def qualitativo(acao: Dict[str, Any], lookback: int = 30):
    return pesquisa_ação(acao, lookback_days=lookback)


@app.post("/daily")
def daily(
    entrada: Dict[str, Any],
    sources: str = "yfinance_only",
    use_alphavantage: bool = False,
    compute_disparity: bool = False,
    drop_empty_kpi_cols: bool = True,
    years_back: int | None = None,
):
    johnson, df = pesquisa_acao_daily_kpis(
        entrada,
        debug=False,
        sources=sources,
        use_alphavantage=use_alphavantage,
        compute_disparity=compute_disparity,
        drop_empty_kpi_cols=drop_empty_kpi_cols,
        years_back=years_back,
    )
    # Retorna só head para não explodir o payload; o front pode pedir CSV à parte se quiser
    return {"johnson": johnson, "df_head": df.head(10).to_dict(orient="records")}


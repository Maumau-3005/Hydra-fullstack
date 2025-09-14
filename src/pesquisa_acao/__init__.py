from .empresa_header import montar_json_empresa  # conveniência

__all__ = ["montar_json_empresa", "pesquisa_ação", "pesquisa_acao_daily_kpis"]

# Lazy export to avoid importing heavy dependencies at package import time.
def __getattr__(name):
    if name == "pesquisa_ação":
        from .pipelines.pesquisa_acao import pesquisa_ação as _fn
        return _fn
    if name == "pesquisa_acao_daily_kpis":
        from .pipelines.pesquisa_acao_daily_kpis import pesquisa_acao_daily_kpis as _fn
        return _fn
    raise AttributeError(name)

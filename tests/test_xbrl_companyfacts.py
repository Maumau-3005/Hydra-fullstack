from pesquisa_acao.pipelines.pesquisa_acao_daily_kpis import CANON_MAP


def test_canon_map_has_core_kpis():
    for k in ["Revenues", "NetIncomeLoss", "OperatingIncomeLoss", "EarningsPerShareDiluted"]:
        assert k in CANON_MAP


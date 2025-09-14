from pesquisa_acao.empresa_header import _normalize_gics


def test_gics_normalization_basic():
    assert _normalize_gics("technology") == "Information Technology"
    assert _normalize_gics("saúde") == "Health Care"
    assert _normalize_gics("Energy") == "Energy"


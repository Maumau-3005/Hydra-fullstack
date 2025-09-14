from pesquisa_acao.empresa_header import _to_iso_date_str


def test_date_parsing():
    assert _to_iso_date_str("2025/09/06") == "2025-09-06"
    assert _to_iso_date_str("06/09/2025") == "2025-09-06"


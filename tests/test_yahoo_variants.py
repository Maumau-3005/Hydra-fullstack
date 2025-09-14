from pesquisa_acao.empresa_header import _guess_yahoo_variants


def test_b3_variants():
    v = _guess_yahoo_variants("PETR3")
    assert "PETR3" in v and "PETR3.SA" in v


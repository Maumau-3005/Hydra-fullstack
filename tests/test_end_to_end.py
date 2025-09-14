from pesquisa_acao.empresa_header import montar_json_empresa


def test_header_smoke():
    js = montar_json_empresa("PETR3", "2025-06-30", "2025-09-06")
    assert "Nome_da_empresa" in js and "Setor_da_empresa" in js


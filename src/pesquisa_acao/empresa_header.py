# -*- coding: utf-8 -*-
"""
empresa_header.py — monta JSON com nome da empresa e setor GICS a partir de um ticker e 2 datas.

Exemplo de retorno:
{
    "codigo_da_ação": "PETR3",
    "Nome_da_empresa": "Petrobras - Petróleo Brasileiro S.A.",
    "Setor_da_empresa": "Energy",
    "Final_do_teste": "2025-06-30",
    "hojé_em_dia": "2025-09-06"
}
"""

from __future__ import annotations
import warnings
from typing import Dict, Optional, List
from datetime import datetime

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

try:
    import yfinance as yf  # pip install yfinance
except Exception:  # pragma: no cover
    yf = None


# ---------------------- Normalização GICS ----------------------
_GICS_SET = {
    "Communication Services",
    "Consumer Discretionary",
    "Consumer Staples",
    "Energy",
    "Financials",
    "Health Care",
    "Industrials",
    "Information Technology",
    "Materials",
    "Real Estate",
    "Utilities",
}

# mapeia variações/comuns -> GICS oficial
_GICS_NORMALIZE = {
    "communication services": "Communication Services",
    "communications": "Communication Services",
    "consumer discretionary": "Consumer Discretionary",
    "consumer staples": "Consumer Staples",
    "energy": "Energy",
    "financials": "Financials",
    "financial services": "Financials",
    "health care": "Health Care",
    "healthcare": "Health Care",
    "industrials": "Industrials",
    "industrial": "Industrials",
    "information technology": "Information Technology",
    "technology": "Information Technology",
    "tech": "Information Technology",
    "materials": "Materials",
    "real estate": "Real Estate",
    "real-estate": "Real Estate",
    "utilities": "Utilities",
    # pt/pt-br ocasionais
    "tecnologia": "Information Technology",
    "saúde": "Health Care",
    "energia": "Energy",
    "financeiro": "Financials",
    "materiais": "Materials",
    "utilidades": "Utilities",
    "bens de consumo": "Consumer Staples",
    "discricionário": "Consumer Discretionary",
    "comunicação": "Communication Services",
    "imobiliário": "Real Estate",
}

def _normalize_gics(sector: Optional[str]) -> Optional[str]:
    if not sector or not isinstance(sector, str):
        return None
    s = sector.strip()
    if s in _GICS_SET:
        return s
    key = s.lower()
    return _GICS_NORMALIZE.get(key, None)


# ---------------------- Heurísticas de símbolo ----------------------
def _guess_yahoo_variants(code: str) -> List[str]:
    """
    Gera variantes prováveis para o Yahoo Finance.
    - B3: código alfanumérico terminado em dígito (ex.: PETR3, VALE3, ITUB4) -> '.SA'
    - Caso já contenha um sufixo (.SA, .NY, .US etc.), usa como está e também tenta sem.
    - Em geral: tenta [code, code+'.SA'] nessa ordem, sem duplicar.
    """
    code = (code or "").strip().upper()
    variants: List[str] = []
    has_dot = "." in code
    # como está
    variants.append(code)
    # heurística B3
    if not has_dot:
        # padrão B3 clássico: 4 letras + 1-2 dígitos (3,4,11 etc.)
        import re
        if re.fullmatch(r"[A-Z]{4}\d{1,2}", code):
            variants.append(f"{code}.SA")
    # se usuário já passou .SA, tentamos também sem
    if has_dot and code.endswith(".SA"):
        base = code.split(".")[0]
        variants.append(base)
    # remove duplicados mantendo a ordem
    out: List[str] = []
    for v in variants:
        if v not in out:
            out.append(v)
    return out


# ---------------------- Fallback local (nomes/sectores) ----------------------
# Pequeno dicionário para casos comuns se yfinance falhar.
_FALLBACK_PROFILE = {
    # B3
    "PETR3": ("Petrobras - Petróleo Brasileiro S.A.", "Energy"),
    "PETR4": ("Petrobras - Petróleo Brasileiro S.A.", "Energy"),
    "VALE3": ("Vale S.A.", "Materials"),
    "ITUB4": ("Itaú Unibanco Holding S.A.", "Financials"),
    "BBDC4": ("Banco Bradesco S.A.", "Financials"),
    "ABEV3": ("Ambev S.A.", "Consumer Staples"),
    "BBAS3": ("Banco do Brasil S.A.", "Financials"),
    "WEGE3": ("WEG S.A.", "Industrials"),
    "ELET3": ("Eletrobras", "Utilities"),
    "SUZB3": ("Suzano S.A.", "Materials"),
    "MGLU3": ("Magazine Luiza S.A.", "Consumer Discretionary"),
    # EUA/ADR exemplos
    "PBR": ("Petróleo Brasileiro S.A. - Petrobras", "Energy"),
    "AAPL": ("Apple Inc.", "Information Technology"),
    "MSFT": ("Microsoft Corporation", "Information Technology"),
    "GOOGL": ("Alphabet Inc.", "Communication Services"),
}


# ---------------------- yfinance: coleta de nome e setor ----------------------
def _get_profile_yfinance(symbol: str) -> Optional[Dict[str, str]]:
    if yf is None:
        return None
    try:
        t = yf.Ticker(symbol)
        # .get_info() evita alguns avisos das versões recentes
        try:
            info = t.get_info()
        except Exception:
            info = getattr(t, "info", {}) or {}
        if not info:
            return None

        # nome preferencial
        name = info.get("longName") or info.get("shortName") or info.get("displayName")
        sector_raw = info.get("sector") or info.get("industry")  # às vezes só vem 'industry'
        sector = _normalize_gics(sector_raw) or _normalize_gics(info.get("industry"))  # segunda chance

        if not name and not sector:
            return None
        return {"name": name, "sector": sector or sector_raw}
    except Exception:
        return None


# ---------------------- Normalização de datas ----------------------
def _to_iso_date_str(d: str) -> str:
    """
    Aceita 'YYYY-MM-DD', 'YYYY/MM/DD', 'DD/MM/YYYY', 'YYYY.MM.DD' etc. Retorna 'YYYY-MM-DD'.
    """
    ds = (d or "").strip()
    # tenta parsing direto
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%Y.%m.%d", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(ds, fmt).date().isoformat()
        except Exception:
            pass
    # fallback: tenta parser genérico do datetime.fromisoformat
    try:
        return datetime.fromisoformat(ds).date().isoformat()
    except Exception:
        # se tudo falhar, devolve a string original (melhor que quebrar)
        return ds


# ---------------------- Função pública ----------------------
def montar_json_empresa(codigo_acao: str, final_do_teste: str, hoje_em_dia: str, *, debug: bool=False) -> Dict[str, str]:
    """
    Retorna um dicionário no formato desejado:
    {
        "codigo_da_ação": "<código original>",
        "Nome_da_empresa": "<nome>",
        "Setor_da_empresa": "<GICS>",
        "Final_do_teste": "<YYYY-MM-DD>",
        "hojé_em_dia": "<YYYY-MM-DD>"
    }
    """
    code_in = (codigo_acao or "").strip().upper()
    # 1) tenta yfinance com variantes
    name: Optional[str] = None
    sector: Optional[str] = None

    for sym in _guess_yahoo_variants(code_in):
        prof = _get_profile_yfinance(sym)
        if prof:
            if debug:
                print(f"[DEBUG] yfinance hit: {sym} -> {prof}")
            name = name or prof.get("name")
            sector = prof.get("sector") or sector
            # se já fechou nome e setor GICS normalizado, para
            if name and _normalize_gics(sector):
                sector = _normalize_gics(sector)
                break

    # 2) fallback local (se ainda faltou algo)
    if (not name or not sector) and code_in in _FALLBACK_PROFILE:
        fb_name, fb_sector = _FALLBACK_PROFILE[code_in]
        if debug:
            print(f"[DEBUG] fallback local usado para {code_in}")
        name = name or fb_name
        sector = sector or fb_sector

    # 3) normaliza setor para GICS (se possível)
    sector_norm = _normalize_gics(sector) or sector or "Unknown"

    # 4) normaliza datas
    d_final = _to_iso_date_str(final_do_teste)
    d_hoje = _to_iso_date_str(hoje_em_dia)

    # formatação simples do nome (opcional)
    def _format_name(n: Optional[str]) -> str:
        if not n:
            return code_in  # último recurso
        # exemplos: "Petróleo Brasileiro S.A. - Petrobras" -> "Petrobras - Petróleo Brasileiro S.A."
        lower = n.lower()
        if "petrobras" in lower and "petróleo brasileiro" in lower and " - " in n:
            parts = [p.strip() for p in n.split(" - ")]
            if len(parts) == 2:
                # inverte para priorizar "Petrobras - ..."
                if "petrobras" in parts[1].lower():
                    return f"{parts[1]} - {parts[0]}"
        return n

    nome_fmt = _format_name(name)

    out = {
        "codigo_da_ação": code_in,
        "Nome_da_empresa": nome_fmt,
        "Setor_da_empresa": sector_norm,
        "Final_do_teste": d_final,
        "hojé_em_dia": d_hoje,
    }
    if debug:
        print(f"[DEBUG] JSON final: {out}")
    return out


# ---------------------- Exemplo rápido ----------------------
if __name__ == "__main__":
    exemplo = montar_json_empresa("PETR3", "2025-06-30", "2025-09-06", debug=True)
    print(exemplo)


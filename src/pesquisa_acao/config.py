from __future__ import annotations
import json, os
from typing import Any, Dict

def load_config() -> Dict[str, Any]:
    """
    Carrega config de PESQ_CONFIG (se existir) ou de ./config.json (repo root).
    Caso nenhum exista, retorna {} — os módulos tratam defaults internamente.
    """
    path = os.getenv("PESQ_CONFIG")
    cand = [path] if path else []
    cand += ["config.json", os.path.join(os.getcwd(), "config.json")]
    for p in cand:
        try:
            if p and os.path.exists(p):
                return json.load(open(p, "r", encoding="utf-8"))
        except Exception:
            pass
    return {}


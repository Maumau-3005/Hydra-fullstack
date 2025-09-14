#!/usr/bin/env python3
import argparse, json, sys
from pesquisa_acao.pipelines.pesquisa_acao import pesquisa_ação

def main():
    ap = argparse.ArgumentParser(description="Pipeline qualitativo de pesquisa de ação")
    ap.add_argument("input_json", help="JSON no formato gerado por empresa_header")
    ap.add_argument("-o", "--out")
    ap.add_argument("--lookback", type=int, default=30)
    args = ap.parse_args()

    entrada = json.load(open(args.input_json, "r", encoding="utf-8"))
    res = pesquisa_ação(entrada, lookback_days=args.lookback)

    if args.out:
        json.dump(res, open(args.out, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    else:
        json.dump(res, sys.stdout, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()


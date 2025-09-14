#!/usr/bin/env python3
import argparse, json
from pesquisa_acao.empresa_header import montar_json_empresa

def main():
    ap = argparse.ArgumentParser(description="Gera JSON de cabeçalho da empresa")
    ap.add_argument("ticker")
    ap.add_argument("final_do_teste")
    ap.add_argument("hoje_em_dia")
    ap.add_argument("-o", "--out")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    js = montar_json_empresa(args.ticker, args.final_do_teste, args.hoje_em_dia, debug=args.debug)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(js, f, ensure_ascii=False, indent=2)
    else:
        print(json.dumps(js, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()


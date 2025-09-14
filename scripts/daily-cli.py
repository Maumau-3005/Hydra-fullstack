#!/usr/bin/env python3
import argparse, json
from pathlib import Path
from pesquisa_acao.pipelines.pesquisa_acao_daily_kpis import pesquisa_acao_daily_kpis

def main():
    ap = argparse.ArgumentParser(description="Série diária de preço + KPIs (lock-to-publication)")
    ap.add_argument("ticker")
    ap.add_argument("--nome", default="")
    ap.add_argument("--setor", default="")
    ap.add_argument("--outdir", default="data/outputs")
    ap.add_argument("--sources", default="yfinance_only", choices=["yfinance_only", "all"])
    ap.add_argument("--use-alphavantage", action="store_true")
    ap.add_argument("--compute-disparity", action="store_true")
    ap.add_argument("--no-drop-empty", dest="drop_empty", action="store_false")
    ap.add_argument("--years-back", type=int, default=None)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    entrada = {
        "codigo_da_ação": args.ticker,
        "Nome_da_empresa": args.nome,
        "Setor_da_empresa": args.setor or None,
    }

    johnson, df = pesquisa_acao_daily_kpis(
        entrada,
        debug=args.debug,
        sources=args.sources,
        use_alphavantage=args.use_alphavantage,
        compute_disparity=args.compute_disparity,
        drop_empty_kpi_cols=args.drop_empty,
        years_back=args.years_back,
    )

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    df.to_csv(outdir / "pesquisa_daily_kpis_df.csv", index=False)
    json.dump(johnson, open(outdir / "pesquisa_daily_kpis_johnson.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=2)
    print(f"Salvo em {outdir}")

if __name__ == "__main__":
    main()


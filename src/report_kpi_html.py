from pathlib import Path
import pandas as pd

CSV = Path("data/gold/kpi_monthly.csv")
OUT = Path("reports/kpi_report.html")

def main() -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)

    if not CSV.exists():
        raise SystemExit(f"Missing input CSV: {CSV}")

    df = pd.read_csv(CSV)

    # basic HTML table
    html_table = df.to_html(index=False)

    page = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Monthly KPI Report</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 40px; }}
    table {{ border-collapse: collapse; }}
    th, td {{ border: 1px solid #ccc; padding: 6px 10px; }}
    th {{ background: #eee; }}
    h1 {{ margin-bottom: 10px; }}
    .meta {{ color: #555; margin-bottom: 18px; }}
  </style>
</head>
<body>
  <h1>Monthly KPI Report</h1>
  <div class="meta">Source: {CSV.as_posix()}</div>
  {html_table}
</body>
</html>
"""

    OUT.write_text(page, encoding="utf-8")
    print(f"OK: wrote {OUT}")

if __name__ == "__main__":
    main()

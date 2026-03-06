from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

IN_TOP = Path("reports/campaign_detector/campaign_spikes_top.csv")
IN_ALL = Path("reports/campaign_detector/campaign_spikes_all.csv")
OUT_DIR = Path("reports/campaign_2025")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def main():
    if not IN_ALL.exists():
        raise SystemExit(f"Missing: {IN_ALL}")
    df = pd.read_csv(IN_ALL)

    # Keep only 2025+
    # year_month is like "2025-04"
    df["year"] = df["year_month"].astype(str).str.slice(0, 4).astype(int)
    df_2025 = df[df["year"] >= 2025].copy()

    # Save filtered data
    df_2025.to_csv(OUT_DIR / "campaign_spikes_all_2025plus.csv", index=False)

    # Pick top 5 spikes within 2025+
    top5 = (df_2025.sort_values(["spike_ratio", "count"], ascending=[False, False])
                  .head(5)
                  .copy())
    top5.to_csv(OUT_DIR / "campaign_spikes_top5_2025plus.csv", index=False)

    # --- Chart 1: top 10 spike_ratio bar (2025+) ---
    top10 = (df_2025.sort_values(["spike_ratio", "count"], ascending=[False, False])
                    .head(10)
                    .copy())

    labels = []
    for _, r in top10.iterrows():
        labels.append(f'{r["PRODUCT_CODE"]} | {r["SHIP_TO_COUNTRY"]} | {r["FAILURE_CODE_1"]}/{r["REPAIR_CODE_1"]} | {r["year_month"]}')

    plt.figure()
    plt.bar(range(len(top10)), top10["spike_ratio"])
    plt.title("Top 10 Campaign Spikes (2025+): spike_ratio")
    plt.xlabel("Group (product | country | failure/repair | month)")
    plt.ylabel("Spike ratio vs previous mean")
    plt.xticks(range(len(top10)), labels, rotation=90)
    plt.tight_layout()
    out1 = OUT_DIR / "top10_spike_ratio_2025plus.png"
    plt.savefig(out1)
    print("Saved:", out1)

    # --- Chart 2: monthly trend for top5 groups ---
    # Build a group key
    key_cols = ["PRODUCT_CODE", "SHIP_TO_COUNTRY", "SOLD_TO_CUSTOMER_NAME", "FAILURE_CODE_1", "REPAIR_CODE_1"]
    df_2025["group_key"] = df_2025[key_cols].astype(str).agg(" | ".join, axis=1)
    top5["group_key"] = top5[key_cols].astype(str).agg(" | ".join, axis=1)
    keys = top5["group_key"].tolist()

    trend = (df_2025[df_2025["group_key"].isin(keys)]
             .groupby(["year_month", "group_key"])["count"]
             .sum()
             .unstack(fill_value=0)
             .sort_index())

    plt.figure()
    trend.plot(kind="line")
    plt.title("Monthly Volume Trend (Top 5 spikes, 2025+)")
    plt.xlabel("Month")
    plt.ylabel("Count")
    plt.xticks(rotation=90)
    plt.tight_layout()
    out2 = OUT_DIR / "top5_monthly_trend_2025plus.png"
    plt.savefig(out2)
    print("Saved:", out2)

    # --- Simple English HTML summary (lightweight) ---
    html = []
    html.append("<html><head><meta charset='utf-8'><title>Campaign Report (2025+)</title></head><body>")
    html.append("<h1>Campaign Report (2025+)</h1>")
    html.append("<p>Source: campaign_detector output (spike detection by month and group).</p>")

    html.append("<h2>Top 10 spikes (2025+)</h2>")
    html.append(top10[[
        "PRODUCT_CODE","SHIP_TO_COUNTRY","SOLD_TO_CUSTOMER_NAME","FAILURE_CODE_1","REPAIR_CODE_1",
        "year_month","count","prev_mean","spike_ratio","spike_delta","group_total"
    ]].to_html(index=False))

    html.append("<h2>Charts</h2>")
    html.append(f"<h3>Top 10 spike ratio</h3><img src='{out1.name}' style='max-width:100%;'>")
    html.append(f"<h3>Top 5 monthly trend</h3><img src='{out2.name}' style='max-width:100%;'>")

    html.append("</body></html>")
    out_html = OUT_DIR / "campaign_report_2025plus.html"
    out_html.write_text("\n".join(html), encoding="utf-8")
    print("Saved:", out_html)

    print("\nDONE: reports/campaign_2025/")

if __name__ == "__main__":
    main()

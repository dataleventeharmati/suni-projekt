from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import matplotlib.pyplot as plt

from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle, PageBreak
)

# ---------- Beállítások ----------
INPUT_PARQUET = Path("data/processed/tch_repairs_cleaned.parquet")
DQ_JSON = Path("reports/data_quality.json")  # ha létezik
OUT_DIR = Path("reports")
CHART_DIR = OUT_DIR / "grafikonok"

OUT_HTML = OUT_DIR / "jelentes.html"
OUT_PDF = OUT_DIR / "jelentes.pdf"

# ---------- Segédek ----------
def ensure_dirs() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    CHART_DIR.mkdir(parents=True, exist_ok=True)

def fmt_int(n: int | float) -> str:
    if n is None or (isinstance(n, float) and pd.isna(n)):
        return "-"
    return f"{int(n):,}".replace(",", " ")

def safe_value_counts(series: pd.Series, top: int = 10) -> pd.Series:
    if series is None:
        return pd.Series(dtype="int64")
    s = series.dropna()
    if s.empty:
        return pd.Series(dtype="int64")
    return s.value_counts().head(top)

def save_bar(series: pd.Series, title: str, xlabel: str, ylabel: str, out_png: Path) -> None:
    plt.figure()
    series.plot(kind="bar")
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(out_png)
    plt.close()

def save_hist(data: pd.Series, title: str, xlabel: str, ylabel: str, out_png: Path, bins: int = 50) -> None:
    plt.figure()
    plt.hist(data.dropna(), bins=bins)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.tight_layout()
    plt.savefig(out_png)
    plt.close()

def html_escape(s: str) -> str:
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )

# ---------- Fő logika ----------
def main() -> None:
    ensure_dirs()

    print("Adatok betöltése...")
    df = pq.read_table(INPUT_PARQUET).to_pandas()

    total_rows = len(df)

    # DQ json (ha van)
    dq = {}
    if DQ_JSON.exists():
        try:
            dq = json.loads(DQ_JSON.read_text(encoding="utf-8"))
        except Exception:
            dq = {}

    # Kötelező dátum oszlopok
    date_cols = ["RMA_DATE", "RECEIVED_DATE", "REPAIR_START_DATE", "REPAIR_END_DATE", "INVOICE_DATE"]
    for c in date_cols:
        if c in df.columns and not pd.api.types.is_datetime64_any_dtype(df[c]):
            df[c] = pd.to_datetime(df[c], errors="coerce")

    # TAT számítás
    if "REPAIR_END_DATE" in df.columns and "RECEIVED_DATE" in df.columns:
        df["TAT_nap"] = (df["REPAIR_END_DATE"] - df["RECEIVED_DATE"]).dt.days
    else:
        df["TAT_nap"] = pd.NA

    tat = pd.to_numeric(df["TAT_nap"], errors="coerce")

    # TAT kategóriák
    bins = [0, 7, 14, 30, 60, 999999]
    labels = ["0-7", "8-14", "15-30", "31-60", "60+"]

    tat_cat = pd.cut(tat.dropna(), bins=bins, labels=labels, right=True)
    tat_dist = tat_cat.value_counts().sort_index()

    # 60+ esetek
    long = df[df["TAT_nap"].notna() & (df["TAT_nap"] > 60)].copy()

    top_failure_60 = safe_value_counts(long.get("FAILURE_CODE_1"), top=10)
    top_repair_60 = safe_value_counts(long.get("REPAIR_CODE_1"), top=10)

    # Drilldown: MM1 + ECN + 60+ + 2025 Q4 (09-12)
    mm1_ecn = df[
        (df["TAT_nap"].notna()) &
        (df["TAT_nap"] > 60) &
        (df.get("FAILURE_CODE_1") == "MM1") &
        (df.get("REPAIR_CODE_1") == "ECN") &
        (df.get("RECEIVED_DATE").notna())
    ].copy()

    mm1_ecn_q4 = mm1_ecn[
        (mm1_ecn["RECEIVED_DATE"].dt.year == 2025) &
        (mm1_ecn["RECEIVED_DATE"].dt.month >= 9) &
        (mm1_ecn["RECEIVED_DATE"].dt.month <= 12)
    ].copy()

    # Monthly trend
    if not mm1_ecn_q4.empty:
        mm1_ecn_q4["honap"] = mm1_ecn_q4["RECEIVED_DATE"].dt.to_period("M").astype(str)
        mm1_ecn_monthly = mm1_ecn_q4["honap"].value_counts().sort_index()
    else:
        mm1_ecn_monthly = pd.Series(dtype="int64")

    # Top dimenziók (egy szinttel mélyebb, ahogy kérted)
    top_product_family = safe_value_counts(mm1_ecn_q4.get("PRODUCT_FAMILY_RECEIVED"), top=10)
    top_product_code = safe_value_counts(mm1_ecn_q4.get("PRODUCT_CODE"), top=10)
    top_country = safe_value_counts(mm1_ecn_q4.get("SHIP_TO_COUNTRY"), top=10)
    top_customer = safe_value_counts(mm1_ecn_q4.get("SOLD_TO_CUSTOMER_NAME"), top=10)

    # Pótalkatrész (ha van)
    spare_ref = safe_value_counts(mm1_ecn_q4.get("SPARE_1_PART_REFERENCE"), top=10)
    spare_desc = safe_value_counts(mm1_ecn_q4.get("SPARE_1_PART_DESCRIPTION"), top=10)

    # TAT stat a fókusz csoporton
    tat_focus = pd.to_numeric(mm1_ecn_q4.get("TAT_nap"), errors="coerce").dropna()
    tat_focus_stats = {}
    if not tat_focus.empty:
        tat_focus_stats = {
            "count": int(len(tat_focus)),
            "min": int(tat_focus.min()),
            "p50": float(tat_focus.median()),
            "p75": float(tat_focus.quantile(0.75)),
            "p90": float(tat_focus.quantile(0.90)),
            "max": int(tat_focus.max()),
        }

    # ---------- Grafikonok ----------
    print("Grafikonok készítése...")
    charts = []

    # 1) TAT kategória distribution
    p1 = CHART_DIR / "tat_kategoria_eloszlas.png"
    if not tat_dist.empty:
        save_bar(tat_dist, "TAT kategória distribution (nap)", "Kategória", "Darab", p1)
        charts.append(p1)

    # 2) TAT hisztogram (vágás: 0..200 nap a láthatóságért)
    p2 = CHART_DIR / "tat_histogram_0_200.png"
    tat_0_200 = tat[(tat >= 0) & (tat <= 200)]
    if not tat_0_200.empty:
        save_hist(tat_0_200, "TAT hisztogram (0-200 nap)", "Nap", "Darab", p2, bins=40)
        charts.append(p2)

    # 3) 60+ top hibakód
    p3 = CHART_DIR / "top_hibakod_60plus.png"
    if not top_failure_60.empty:
        save_bar(top_failure_60, "Top 10 FAILURE_CODE_1 (60+ nap)", "Hibakód", "Darab", p3)
        charts.append(p3)

    # 4) MM1+ECN (60+) havi trend (2025 Q4)
    p4 = CHART_DIR / "mm1_ecn_60plus_havi_2025q4.png"
    if not mm1_ecn_monthly.empty:
        save_bar(mm1_ecn_monthly, "MM1 + ECN (60+ nap) havi distribution (2025 Q4)", "Hónap", "Darab", p4)
        charts.append(p4)

    # ---------- HTML ----------
    print("HTML generálása...")
    html_parts = []
    html_parts.append("<!doctype html><html><head><meta charset='utf-8'>")
    html_parts.append("""
    <style>
      body { font-family: Arial, sans-serif; margin: 24px; }
      h1 { margin-bottom: 6px; }
      .mutato { display: inline-block; padding: 10px 12px; margin: 6px 10px 6px 0; border: 1px solid #ddd; border-radius: 10px; }
      .grid { display: grid; grid-template-columns: 1fr; gap: 14px; }
      img { max-width: 100%; border: 1px solid #eee; border-radius: 10px; padding: 6px; background: #fff; }
      table { border-collapse: collapse; margin-top: 8px; }
      td, th { border: 1px solid #ddd; padding: 6px 8px; font-size: 13px; }
      th { background: #f4f4f4; text-align: left; }
      .small { color: #666; font-size: 12px; }
    </style>
    """)
    html_parts.append("</head><body>")
    html_parts.append("<h1>TCH Repair Data Analysis Report</h1>")
    html_parts.append("<div class='small'>Forrás: tch_repairs_cleaned.parquet</div>")

    # mutatók
    pk_dup = dq.get("duplicate_primary_keys", None)
    html_parts.append("<div>")
    html_parts.append(f"<div class='mutato'><b>Total Records</b><br>{fmt_int(total_rows)}</div>")
    html_parts.append(f"<div class='mutato'><b>Primary Key Duplicates</b><br>{fmt_int(pk_dup) if pk_dup is not None else '-'}</div>")
    html_parts.append(f"<div class='mutato'><b>60+ Days Cases</b><br>{fmt_int(len(long))}</div>")
    html_parts.append(f"<div class='mutato'><b>MM1+ECN 60+ (2025 Q4)</b><br>{fmt_int(len(mm1_ecn_q4))}</div>")
    html_parts.append("</div>")

    # dátum nullok
    html_parts.append("<h2>Date Fields – Null Values (parse után)</h2>")
    date_nulls = {}
    for c in date_cols:
        if c in df.columns:
            date_nulls[c] = int(df[c].isna().sum())
    html_parts.append("<table><tr><th>Mező</th><th>Null Count</th></tr>")
    for k, v in date_nulls.items():
        html_parts.append(f"<tr><td>{html_escape(k)}</td><td>{fmt_int(v)}</td></tr>")
    html_parts.append("</table>")

    # top listák (fókusz)
    def add_top_table(title: str, series: pd.Series):
        html_parts.append(f"<h3>{html_escape(title)}</h3>")
        if series is None or series.empty:
            html_parts.append("<div class='small'>Nincs adat.</div>")
            return
        html_parts.append("<table><tr><th>Érték</th><th>Darab</th></tr>")
        for idx, val in series.items():
            html_parts.append(f"<tr><td>{html_escape(idx)}</td><td>{fmt_int(val)}</td></tr>")
        html_parts.append("</table>")

    html_parts.append("<h2>Focus: MM1 + ECN (60+ nap) – 2025 Q4</h2>")
    if tat_focus_stats:
        html_parts.append("<table><tr><th>Mutató</th><th>Érték</th></tr>")
        for k in ["count", "min", "p50", "p75", "p90", "max"]:
            html_parts.append(f"<tr><td>{k}</td><td>{html_escape(tat_focus_stats[k])}</td></tr>")
        html_parts.append("</table>")

    add_top_table("Top termékcsalád (PRODUCT_FAMILY_RECEIVED)", top_product_family)
    add_top_table("Top termékkód (PRODUCT_CODE)", top_product_code)
    add_top_table("Top ország (SHIP_TO_COUNTRY)", top_country)
    add_top_table("Top vevő (SOLD_TO_CUSTOMER_NAME)", top_customer)
    add_top_table("Top pótalkatrész (SPARE_1_PART_REFERENCE)", spare_ref)
    add_top_table("Top pótalkatrész leírás (SPARE_1_PART_DESCRIPTION)", spare_desc)

    # grafikonok
    html_parts.append("<h2>Grafikonok</h2><div class='grid'>")
    for p in charts:
        rel = p.relative_to(OUT_DIR)
        html_parts.append(f"<div><img src='{html_escape(rel.as_posix())}' alt='{html_escape(p.stem)}'></div>")
    html_parts.append("</div>")

    html_parts.append("</body></html>")
    OUT_HTML.write_text("\n".join(html_parts), encoding="utf-8")

    # ---------- PDF ----------
    print("PDF generálása...")
    styles = getSampleStyleSheet()
    doc = SimpleDocTemplate(str(OUT_PDF), pagesize=A4, leftMargin=2*cm, rightMargin=2*cm, topMargin=2*cm, bottomMargin=2*cm)
    story = []

    # ---------- Table of Contents (auto) ----------
    def add_table_of_contents(story, styles):
        story.append(Paragraph("Table of Contents", styles["Title"]))
        story.append(Spacer(1, 12))

        toc_items = [
            "Executive Highlights",
            "Campaign Spike Deep-Dive (Top 5)",
            "Key Risks & Recommendations",
            "Core KPI Overview",
            "Focus Analysis: MM1 + ECN (60+ days)",
            "Top Dimensions",
            "Charts"
        ]

        for item in toc_items:
            story.append(Paragraph(f"• {item}", styles["Normal"]))
            story.append(Spacer(1, 6))

        story.append(PageBreak())

    add_table_of_contents(story, styles)
    # ---------- /Table of Contents ----------

    # ---------- Executive Highlights (auto) ----------
    EH_MD = OUT_DIR / "executive_highlights.md"

    def add_exec_highlights(story, styles):
        """
        Minimal MD -> ReportLab Paragraph renderer for executive_highlights.md
        Supports: #/##/### headings, '- ' bullets, and _italic_ single-line notes.
        """
        if not EH_MD.exists():
            return

        story.append(Paragraph("Executive Highlights", styles["Title"]))
        lines = EH_MD.read_text(encoding="utf-8").splitlines()

        for line in lines:
            line = line.rstrip()
            if not line.strip():
                story.append(Spacer(1, 6))
                continue

            if line.startswith("# "):
                story.append(Paragraph(line[2:].strip(), styles["Title"]))
                story.append(Spacer(1, 6))
                continue
            if line.startswith("## "):
                story.append(Paragraph(line[3:].strip(), styles["Heading2"]))
                continue
            if line.startswith("### "):
                story.append(Paragraph(line[4:].strip(), styles["Heading3"]))
                continue

            # italic single-line note like: _Generated: ..._
            if line.startswith("_") and line.endswith("_") and len(line) > 2:
                inner = line[1:-1]
                story.append(Paragraph(f"<i>{inner}</i>", styles["Normal"]))
                continue

            # bullets
            if line.startswith("- "):
                txt = line[2:].strip()
                story.append(Paragraph(f"• {txt}", styles["Normal"]))
                continue

            story.append(Paragraph(line, styles["Normal"]))

        story.append(PageBreak())

    add_exec_highlights(story, styles)
    # ---------- /Executive Highlights ----------

    # ---------- Campaign Deep-Dive (auto) ----------
    DD_MD = OUT_DIR / "campaign_deep_dive.md"

    def add_campaign_deep_dive(story, styles):
        """
        Minimal MD -> ReportLab Paragraph renderer for campaign_deep_dive.md
        Supports: #/##/### headings, '- ' bullets, and _italic_ single-line notes.
        """
        if not DD_MD.exists():
            return

        lines = DD_MD.read_text(encoding="utf-8").splitlines()

        for line in lines:
            line = line.rstrip()
            if not line.strip():
                story.append(Spacer(1, 6))
                continue

            if line.startswith("# "):
                story.append(Paragraph(line[2:].strip(), styles["Title"]))
                story.append(Spacer(1, 6))
                continue
            if line.startswith("## "):
                story.append(Paragraph(line[3:].strip(), styles["Heading2"]))
                continue
            if line.startswith("### "):
                story.append(Paragraph(line[4:].strip(), styles["Heading3"]))
                continue

            if line.startswith("_") and line.endswith("_") and len(line) > 2:
                inner = line[1:-1]
                story.append(Paragraph(f"<i>{inner}</i>", styles["Normal"]))
                continue

            if line.startswith("- "):
                txt = line[2:].strip()
                story.append(Paragraph(f"• {txt}", styles["Normal"]))
                continue

            if line.strip() == "---":
                story.append(Spacer(1, 10))
                continue

            story.append(Paragraph(line, styles["Normal"]))

        story.append(PageBreak())

    add_campaign_deep_dive(story, styles)
    # ---------- /Campaign Deep-Dive ----------

    # ---------- Key Risks & Recommendations (auto) ----------
    KR_MD = OUT_DIR / "key_risks_recommendations.md"

    def add_key_risks_recommendations(story, styles):
        """
        Minimal MD -> ReportLab Paragraph renderer for key_risks_recommendations.md
        Supports: #/##/### headings, '- ' bullets, and _italic_ single-line notes.
        """
        if not KR_MD.exists():
            return

        lines = KR_MD.read_text(encoding="utf-8").splitlines()

        for line in lines:
            line = line.rstrip()
            if not line.strip():
                story.append(Spacer(1, 6))
                continue

            if line.startswith("# "):
                story.append(Paragraph(line[2:].strip(), styles["Title"]))
                story.append(Spacer(1, 6))
                continue
            if line.startswith("## "):
                story.append(Paragraph(line[3:].strip(), styles["Heading2"]))
                continue
            if line.startswith("### "):
                story.append(Paragraph(line[4:].strip(), styles["Heading3"]))
                continue

            if line.startswith("_") and line.endswith("_") and len(line) > 2:
                inner = line[1:-1]
                story.append(Paragraph(f"<i>{inner}</i>", styles["Normal"]))
                continue

            if line.startswith("- "):
                txt = line[2:].strip()
                story.append(Paragraph(f"• {txt}", styles["Normal"]))
                continue

            if line.strip() == "---":
                story.append(Spacer(1, 10))
                continue

            story.append(Paragraph(line, styles["Normal"]))

        story.append(PageBreak())

    add_key_risks_recommendations(story, styles)
    # ---------- /Key Risks & Recommendations ----------

    story.append(Paragraph("TCH Repair Data Analysis Report", styles["Title"]))
    story.append(Paragraph("Forrás: tch_repairs_cleaned.parquet", styles["Normal"]))
    story.append(Spacer(1, 10))

    # KPI tábla
    kpi_data = [
        ["Mutató", "Érték"],
        ["Total Records", fmt_int(total_rows)],
        ["Primary Key Duplicates", fmt_int(pk_dup) if pk_dup is not None else "-"],
        ["60+ Days Cases", fmt_int(len(long))],
        ["MM1+ECN 60+ (2025 Q4)", fmt_int(len(mm1_ecn_q4))],
    ]
    kpi_tbl = Table(kpi_data, hAlign="LEFT", colWidths=[7*cm, 6*cm])
    kpi_tbl.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.lightgrey),
        ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
        ("PADDING", (0,0), (-1,-1), 6),
    ]))
    story.append(kpi_tbl)
    story.append(Spacer(1, 12))

    # Dátum nullok
    story.append(Paragraph("Date Fields – Null Values (parse után)", styles["Heading2"]))
    dn_data = [["Mező", "Null Count"]] + [[k, fmt_int(v)] for k, v in date_nulls.items()]
    dn_tbl = Table(dn_data, hAlign="LEFT", colWidths=[7*cm, 6*cm])
    dn_tbl.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.lightgrey),
        ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
        ("PADDING", (0,0), (-1,-1), 6),
    ]))
    story.append(dn_tbl)
    story.append(Spacer(1, 12))

    # Focus blokk
    story.append(Paragraph("Focus: MM1 + ECN (60+ nap) – 2025 Q4", styles["Heading2"]))
    if tat_focus_stats:
        f_data = [["Mutató", "Érték"]] + [[k, str(tat_focus_stats[k])] for k in ["count", "min", "p50", "p75", "p90", "max"]]
        f_tbl = Table(f_data, hAlign="LEFT", colWidths=[7*cm, 6*cm])
        f_tbl.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), colors.lightgrey),
            ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
            ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
            ("PADDING", (0,0), (-1,-1), 6),
        ]))
        story.append(f_tbl)
        story.append(Spacer(1, 10))

    def add_small_top(title: str, series: pd.Series):
        story.append(Paragraph(title, styles["Heading3"]))
        if series is None or series.empty:
            story.append(Paragraph("Nincs adat.", styles["Normal"]))
            story.append(Spacer(1, 6))
            return
        data = [["Érték", "Darab"]] + [[str(idx), fmt_int(val)] for idx, val in series.items()]
        tbl = Table(data, hAlign="LEFT", colWidths=[9*cm, 4*cm])
        tbl.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), colors.lightgrey),
            ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
            ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
            ("PADDING", (0,0), (-1,-1), 6),
        ]))
        story.append(tbl)
        story.append(Spacer(1, 10))

    add_small_top("Top termékkód (PRODUCT_CODE)", top_product_code)
    add_small_top("Top ország (SHIP_TO_COUNTRY)", top_country)
    add_small_top("Top pótalkatrész (SPARE_1_PART_REFERENCE)", spare_ref)

    story.append(PageBreak())
    story.append(Paragraph("Grafikonok", styles["Heading2"]))
    story.append(Spacer(1, 8))

    for p in charts:
        story.append(Paragraph(p.stem.replace("_", " "), styles["Heading3"]))
        img = Image(str(p))
        img.drawWidth = 16*cm
        img.drawHeight = img.drawHeight * (img.drawWidth / img.drawWidth)  # stabil
        story.append(img)
        story.append(Spacer(1, 10))

    doc.build(story)

    print("\nKész.")
    print(f"HTML: {OUT_HTML}")
    print(f"PDF : {OUT_PDF}")
    print(f"Grafikonok: {CHART_DIR}")

if __name__ == "__main__":
    main()
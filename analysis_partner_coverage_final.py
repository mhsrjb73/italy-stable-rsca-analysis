import os
import pandas as pd
from io import StringIO

print("PARTNER_COVERAGE_FINAL = START")

# =========================
# FIXED DATA DIRECTORY
# =========================
BASE_DIR = "/Users/mahsarajabi/Downloads/italy"

# =========================
# LOAD STABLE HS6 LIST
# =========================
stable_path = os.path.join(BASE_DIR, "italy_hs6_stable_min3years_avg_rsca.csv")
stable = pd.read_csv(stable_path)
stable["hs6"] = stable["hs6"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(6)
stable_hs6 = set(stable["hs6"])

print("Stable HS6 count:", len(stable_hs6))

# =========================
# PARTNER FILES (EXPLICIT)
# =========================
partner_files = {
    "Germany": "italy to germany.xls",
    "France": "italy to france.xls",
    "Spain": "italy to spain.xls",
    "Switzerland": "italy to switzerland.xls",
    "Poland": "italy to poland.xls",
    "Belgium": "_Italy_and_Belgium.xls",
    "Netherlands": "Italy_and_Netherlands.xls",
    "Austria": "Italy_and_Austria.xls",
    "Romania": "Italy_and_Romania.xls",
    "Czech Republic": "Italy_and_Czech_Republic .xls"
}

# =========================
# ROBUST TRADEMAP READER
# =========================
def read_trademap_xls(path):
    # 1) Try Excel engines
    engines = [None, "calamine", "xlrd", "openpyxl"]
    for eng in engines:
        try:
            if eng:
                df = pd.read_excel(path, engine=eng)
            else:
                df = pd.read_excel(path)
            if df is not None and df.shape[0] > 0:
                return df
        except Exception:
            pass

    # 2) Fallback: HTML inside XLS
    for enc in ["utf-8", "latin-1", "cp1252"]:
        try:
            with open(path, "r", encoding=enc, errors="ignore") as f:
                html = f.read()
            tables = pd.read_html(StringIO(html))
            if tables:
                return max(tables, key=lambda x: x.shape[0])
        except Exception:
            pass

    raise RuntimeError(f"Cannot read TradeMap file: {path}")

# =========================
# NORMALIZE HS6
# =========================
def normalize_hs6(series):
    return (
        series.astype(str)
        .str.replace(r"\D", "", regex=True)
        .str.zfill(6)
    )

# =========================
# MAIN COVERAGE CALCULATION
# =========================
rows = []

for partner, fname in partner_files.items():
    path = os.path.join(BASE_DIR, fname)
    print(f"Processing: {partner}")

    df = read_trademap_xls(path)

    # Detect HS column
    hs_col = None
    for c in df.columns:
        cl = str(c).lower()
        if "product code" in cl or cl.strip() == "code":
            hs_col = c
            break

    if hs_col is None:
        raise ValueError(f"HS code column not found in {fname}")

    df["hs6"] = normalize_hs6(df[hs_col])

    exported_codes = set(df["hs6"].unique())
    covered_codes = stable_hs6.intersection(exported_codes)

    rows.append({
        "partner": partner,
        "stable_hs6_total": len(stable_hs6),
        "stable_hs6_exported": len(covered_codes),
        "coverage_ratio": len(covered_codes) / len(stable_hs6)
    })

# =========================
# SAVE OUTPUT
# =========================
out = pd.DataFrame(rows).sort_values("coverage_ratio", ascending=False)

output_path = os.path.join(
    BASE_DIR, "italy_stable_rsca_partner_coverage.csv"
)
out.to_csv(output_path, index=False)

print("DONE ✔")
print(out)
print("Saved to:", output_path)

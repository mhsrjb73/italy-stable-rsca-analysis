import os
import re
import pandas as pd
from io import StringIO

print("PARTNER_COVERAGE_VALUE_GT0 = START")

BASE_DIR = os.path.expanduser("~/Downloads/italy")

STABLE_FILE = os.path.join(BASE_DIR, "italy_hs6_stable_min3years_avg_rsca.csv")

PARTNER_FILES = {
    "Germany": "italy to germany.xls",
    "France": "italy to france.xls",
    "Spain": "italy to spain.xls",
    "Switzerland": "italy to switzerland.xls",
    "Poland": "italy to poland.xls",
    "Belgium": "_Italy_and_Belgium.xls",
    "Netherlands": "Italy_and_Netherlands.xls",
    "Austria": "Italy_and_Austria.xls",
    "Romania": "Italy_and_Romania.xls",
    "Czech Republic": "Italy_and_Czech_Republic .xls",
}

YEAR_MIN = 2013
YEAR_MAX = 2024
YEARS = list(range(YEAR_MIN, YEAR_MAX + 1))

def normalize_hs6(s):
    return (
        s.astype(str)
        .str.replace(r"\D", "", regex=True)
        .str.zfill(6)
    )

def read_trademap_html(path):
    for enc in ["utf-8", "latin-1", "cp1252"]:
        try:
            with open(path, "r", encoding=enc, errors="ignore") as f:
                html = f.read()
            tables = pd.read_html(StringIO(html))
            if tables:
                return max(tables, key=lambda x: x.shape[0])
        except Exception:
            continue
    raise RuntimeError(f"Cannot parse HTML table in {path}")

def detect_hs_col(df):
    best_col, best_score = None, -1
    for c in df.columns:
        ser = normalize_hs6(df[c])
        valid = ser.str.match(r"^\d{6}$") & (ser != "000000")
        score = int(valid.sum())
        if score > best_score:
            best_col, best_score = c, score
    if best_col is None:
        raise ValueError("HS6 column not detected")
    return best_col

def to_number(x):
    if pd.isna(x):
        return 0.0
    s = str(x).replace(",", "").strip()
    s = re.sub(r"[^0-9.\-]", "", s)
    if s in {"", "-", "."}:
        return 0.0
    try:
        return float(s)
    except:
        return 0.0

stable = pd.read_csv(STABLE_FILE)
stable["hs6"] = normalize_hs6(stable["hs6"])
stable_hs6 = set(stable["hs6"])

print("Stable HS6 count:", len(stable_hs6))

rows = []

for partner, fname in PARTNER_FILES.items():
    print("Processing:", partner)
    df = read_trademap_html(os.path.join(BASE_DIR, fname))
    hs_col = detect_hs_col(df)
    df["hs6"] = normalize_hs6(df[hs_col])

    year_cols = [c for c in df.columns if any(str(y) in str(c) for y in YEARS)]
    if not year_cols:
        raise ValueError(f"No year columns found in {fname}")

    values = df[year_cols].applymap(to_number)
    exported = values.gt(0).any(axis=1)

    exported_codes = set(df.loc[exported, "hs6"])
    covered = stable_hs6.intersection(exported_codes)

    rows.append({
        "partner": partner,
        "stable_hs6_total": len(stable_hs6),
        "stable_hs6_exported_value_gt0": len(covered),
        "coverage_ratio": len(covered) / len(stable_hs6),
    })

out = pd.DataFrame(rows).sort_values("coverage_ratio", ascending=False)
out_path = os.path.join(BASE_DIR, "italy_stable_rsca_partner_coverage_value_gt0.csv")
out.to_csv(out_path, index=False)

print("DONE ✔")
print(out)
print("Saved:", out_path)


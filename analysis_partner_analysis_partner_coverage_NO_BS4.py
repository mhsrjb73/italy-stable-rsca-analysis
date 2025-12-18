import os
import pandas as pd
from io import StringIO

print("PARTNER_COVERAGE_NO_BS4 = START")

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
    best_col = None
    best_score = -1
    for c in df.columns:
        ser = normalize_hs6(df[c])
        valid = ser.str.match(r"^\d{6}$") & (ser != "000000")
        score = int(valid.sum())
        if score > best_score:
            best_score = score
            best_col = c
    if best_col is None or best_score <= 0:
        raise ValueError(f"HS column not detected. Columns={list(df.columns)}")
    return best_col

stable = pd.read_csv(STABLE_FILE)
stable["hs6"] = normalize_hs6(stable["hs6"])
stable_hs6 = set(stable["hs6"])

print("Stable HS6 count:", len(stable_hs6))

rows = []

for partner, fname in PARTNER_FILES.items():
    path = os.path.join(BASE_DIR, fname)
    print("Processing:", partner)

    df = read_trademap_html(path)
    hs_col = detect_hs_col(df)

    df["hs6"] = normalize_hs6(df[hs_col])
    exported = set(df["hs6"].unique())
    covered = stable_hs6.intersection(exported)

    rows.append({
        "partner": partner,
        "stable_hs6_total": len(stable_hs6),
        "stable_hs6_exported": len(covered),
        "coverage_ratio": len(covered) / len(stable_hs6),
    })

out = pd.DataFrame(rows).sort_values("coverage_ratio", ascending=False)
out_path = os.path.join(BASE_DIR, "italy_stable_rsca_partner_coverage.csv")
out.to_csv(out_path, index=False)

print("DONE ✔")
print(out)
print("Saved:", out_path)


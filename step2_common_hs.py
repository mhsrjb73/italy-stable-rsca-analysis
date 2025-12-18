import os
import re
import pandas as pd
from io import StringIO

print("STEP2_COMMON_HS = START")

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

YEAR_MIN, YEAR_MAX = 2013, 2024
YEARS = list(range(YEAR_MIN, YEAR_MAX + 1))

def normalize_hs6_series(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
        .str.replace(r"\D", "", regex=True)
        .str.zfill(6)
    )

def to_number(x) -> float:
    if pd.isna(x):
        return 0.0
    s = str(x).strip()
    if s == "" or s.lower() in {"-", "n/a", "na", "null"}:
        return 0.0
    s = s.replace(" ", "").replace(",", "")
    s = re.sub(r"[^0-9\.\-]", "", s)
    if s in {"", "-", "."}:
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0

def read_trademap_html_main_table(path: str) -> pd.DataFrame:
    for enc in ["utf-8", "latin-1", "cp1252"]:
        try:
            with open(path, "r", encoding=enc, errors="ignore") as f:
                html = f.read()
            tables = pd.read_html(StringIO(html))
            if tables:
                return max(tables, key=lambda x: x.shape[0])
        except Exception:
            continue
    raise RuntimeError(f"Cannot parse HTML tables: {path}")

def fix_header_two_rows(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.shape[0] < 3:
        return raw
    c00 = str(raw.iloc[0, 0]).strip().lower()
    c01 = str(raw.iloc[0, 1]).strip().lower()
    if "product code" not in c00 or "product label" not in c01:
        return raw

    top = raw.iloc[0].astype(str)
    sub = raw.iloc[1].astype(str)

    new_cols = []
    for t, s in zip(top, sub):
        t = str(t).strip()
        s = str(s).strip()
        if s.lower() in {"nan", "none"} or s == "":
            new_cols.append(t)
        else:
            new_cols.append(f"{t} | {s}")

    df = raw.iloc[2:].copy()
    df.columns = new_cols
    return df.reset_index(drop=True)

def find_hs_col(df: pd.DataFrame) -> str:
    for c in df.columns:
        if str(c).strip() == "Product code" or str(c).startswith("Product code |"):
            return c
    # fallback by quality
    best_col, best_score = None, -1
    for c in df.columns:
        ser = normalize_hs6_series(df[c])
        valid = ser.str.match(r"^\d{6}$") & (ser != "000000")
        score = int(valid.sum())
        if score > best_score:
            best_score, best_col = score, c
    if best_col is None or best_score <= 0:
        raise ValueError(f"HS column not detected. columns={list(df.columns)}")
    return best_col

def partner_value_cols(df: pd.DataFrame, partner: str) -> dict:
    prefix = f"Italy's exports to {partner}".lower()
    year_to_col = {}
    for c in df.columns:
        name = str(c).lower()
        if prefix in name and "value in" in name:
            for y in YEARS:
                if str(y) in name:
                    year_to_col[y] = c
    return year_to_col

# ---- load stable list
stable = pd.read_csv(STABLE_FILE)
stable["hs6"] = normalize_hs6_series(stable["hs6"])
stable_hs6 = sorted(set(stable["hs6"]))
stable_set = set(stable_hs6)
print("Stable HS6 count:", len(stable_set))

# Partner -> exported stable HS6 set (value>0 in any year)
partner_exported = {}

for partner, fname in PARTNER_FILES.items():
    print("Processing:", partner)
    path = os.path.join(BASE_DIR, fname)

    raw = read_trademap_html_main_table(path)
    df = fix_header_two_rows(raw)

    hs_col = find_hs_col(df)
    df["hs6"] = normalize_hs6_series(df[hs_col])
    df = df[(df["hs6"].str.match(r"^\d{6}$")) & (df["hs6"] != "000000")].copy()

    ycols = partner_value_cols(df, partner)
    if not ycols:
        raise ValueError(f"No partner year columns detected for {partner} in {fname}")

    # numeric values for available years
    available_years = sorted(ycols.keys())
    vals = pd.DataFrame({y: df[ycols[y]].map(to_number) for y in available_years})

    exported_mask = (vals > 0).any(axis=1)
    exported_codes = set(df.loc[exported_mask, "hs6"].unique())
    exported_stable = exported_codes.intersection(stable_set)

    partner_exported[partner] = exported_stable

# ---- build HS6 frequency table across partners
records = []
partners = list(PARTNER_FILES.keys())

for hs in stable_hs6:
    present = [p for p in partners if hs in partner_exported[p]]
    records.append({
        "hs6": hs,
        "partner_count": len(present),
        "partners": "; ".join(present)
    })

freq = pd.DataFrame(records).sort_values(["partner_count", "hs6"], ascending=[False, True])

# ---- binary matrix (Partner x HS6)
mat = pd.DataFrame(index=partners, columns=stable_hs6, data=0, dtype=int)
for p in partners:
    for hs in partner_exported[p]:
        mat.loc[p, hs] = 1

# ---- common sets
common_ge3 = freq[freq["partner_count"] >= 3].copy()
common_ge5 = freq[freq["partner_count"] >= 5].copy()
common_all10 = freq[freq["partner_count"] == len(partners)].copy()

# ---- save outputs
freq_path = os.path.join(BASE_DIR, "step2_hs6_partner_frequency.csv")
mat_path = os.path.join(BASE_DIR, "step2_partner_hs6_matrix_binary.csv")
ge3_path = os.path.join(BASE_DIR, "step2_common_hs6_ge3.csv")
ge5_path = os.path.join(BASE_DIR, "step2_common_hs6_ge5.csv")
all10_path = os.path.join(BASE_DIR, "step2_common_hs6_all10.csv")

freq.to_csv(freq_path, index=False)
mat.to_csv(mat_path, index=True)
common_ge3.to_csv(ge3_path, index=False)
common_ge5.to_csv(ge5_path, index=False)
common_all10.to_csv(all10_path, index=False)

print("DONE ✔")
print("Saved:", freq_path)
print("Saved:", mat_path)
print("Saved:", ge3_path)
print("Saved:", ge5_path)
print("Saved:", all10_path)

print("\nQuick stats:")
print("HS6 exported to >=3 partners:", len(common_ge3))
print("HS6 exported to >=5 partners:", len(common_ge5))
print("HS6 exported to all 10 partners:", len(common_all10))
PY


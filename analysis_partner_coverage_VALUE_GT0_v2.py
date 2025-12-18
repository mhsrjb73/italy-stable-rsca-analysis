import os
import re
import pandas as pd
from io import StringIO

print("PARTNER_COVERAGE_VALUE_GT0_V2 = START")

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
    # TradeMap .xls is HTML
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
    """
    In your files, row 0 and row 1 contain the real header.
    Example:
      row0: "Italy's exports to Germany" repeated
      row1: "Value in 2013" ... "Value in 2024"
    We combine them into one column name, then drop first 2 rows.
    """
    if raw.shape[0] < 3:
        raise ValueError("Table too small to contain 2-row header.")

    # check if looks like your example
    cell00 = str(raw.iloc[0, 0]).strip().lower()
    cell01 = str(raw.iloc[0, 1]).strip().lower()
    if "product code" not in cell00 or "product label" not in cell01:
        # If not in that format, keep as-is
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
    df = df.reset_index(drop=True)
    return df

def find_hs_col(df: pd.DataFrame) -> str:
    # Prefer exact "Product code" (possibly with pipe)
    for c in df.columns:
        if str(c).strip() == "Product code":
            return c
        if str(c).startswith("Product code |"):
            return c
    # Fallback: best quality column
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
    """
    Returns mapping year->column for Italy's exports to {partner}.
    We match columns like:
      "Italy's exports to Germany | Value in 2013"
    """
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
if "hs6" not in stable.columns:
    raise ValueError("Stable file must have a column named 'hs6'.")

stable["hs6"] = normalize_hs6_series(stable["hs6"])
stable_hs6 = set(stable["hs6"])
print("Stable HS6 count:", len(stable_hs6))

rows = []

for partner, fname in PARTNER_FILES.items():
    path = os.path.join(BASE_DIR, fname)
    print("Processing:", partner)

    raw = read_trademap_html_main_table(path)
    df = fix_header_two_rows(raw)

    hs_col = find_hs_col(df)
    df["hs6"] = normalize_hs6_series(df[hs_col])

    ycols = partner_value_cols(df, partner)
    missing = [y for y in YEARS if y not in ycols]
    if len(ycols) == 0:
        # Hard fail with helpful info
        raise ValueError(
            f"No partner year columns detected for {partner} in {fname}. "
            f"Example columns: {list(df.columns)[:10]}"
        )
    if missing:
        print(f"WARNING [{partner}]: missing years: {missing}")

    available_years = sorted(ycols.keys())
    vals = pd.DataFrame({y: df[ycols[y]].map(to_number) for y in available_years})

    exported_mask = (vals > 0).any(axis=1)

    exported_codes = set(df.loc[exported_mask, "hs6"].unique())
    exported_codes.discard("000000")  # remove TOTAL/empty

    covered_codes = stable_hs6.intersection(exported_codes)

    rows.append({
        "partner": partner,
        "stable_hs6_total": len(stable_hs6),
        "stable_hs6_exported_value_gt0": len(covered_codes),
        "coverage_ratio": len(covered_codes) / len(stable_hs6),
        "years_detected": len(available_years),
    })

out = pd.DataFrame(rows).sort_values("coverage_ratio", ascending=False)

out_path = os.path.join(BASE_DIR, "italy_stable_rsca_partner_coverage_value_gt0.csv")
out.to_csv(out_path, index=False)

print("DONE ✔")
print(out)
print("Saved:", out_path)


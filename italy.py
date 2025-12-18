import os
import pandas as pd
import numpy as np
from io import StringIO

print("ITALY_RCA_SCRIPT_VERSION = FINAL_FULL_2025-12-15")

# ===============================
# 1) Paths (همه چیز داخل همین پوشه است)
# ===============================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

ITALY_FILE = os.path.join(
    BASE_DIR,
    "Trade_Map_-_List_of_exported_products_for_the_selected_product_(All_products).xls"
)
WORLD_FILE = os.path.join(
    BASE_DIR,
    "6 digit export.xls"
)

# ===============================
# 2) Read TradeMap-like files (.xls may be Excel or HTML)
# ===============================
def read_as_excel(path: str) -> pd.DataFrame:
    engines = [None, "xlrd", "openpyxl", "calamine"]
    last_err = None
    for eng in engines:
        try:
            if eng is None:
                df = pd.read_excel(path)
            else:
                df = pd.read_excel(path, engine=eng)
            if df is not None and df.shape[0] > 0:
                return df
        except Exception as e:
            last_err = e
    raise RuntimeError(f"Excel read failed: {path} | last error: {last_err}")

def read_as_html_table(path: str) -> pd.DataFrame:
    encodings = ["utf-8", "utf-16", "latin-1", "cp1252"]
    last_err = None
    for enc in encodings:
        try:
            with open(path, "r", encoding=enc, errors="ignore") as f:
                html = f.read()
            tables = pd.read_html(StringIO(html))
            if tables and len(tables) > 0:
                return max(tables, key=lambda x: x.shape[0])
        except Exception as e:
            last_err = e
    raise RuntimeError(f"HTML read failed: {path} | last error: {last_err}")

def read_trademap_main_table(path: str) -> pd.DataFrame:
    # Excel first, then HTML
    try:
        return read_as_excel(path)
    except Exception:
        return read_as_html_table(path)

# ===============================
# 3) Load data
# ===============================
italy_df = read_trademap_main_table(ITALY_FILE)
world_df = read_trademap_main_table(WORLD_FILE)

print("Loaded Italy table shape:", italy_df.shape)
print("Loaded World table shape:", world_df.shape)

# ===============================
# 4) Detect columns
# ===============================
def find_col(cols, must_contain):
    for c in cols:
        s = str(c).lower()
        if all(k.lower() in s for k in must_contain):
            return c
    return None

def guess_year_value_cols(df: pd.DataFrame):
    # columns like "Exported value in 2013" ... "Exported value in 2024"
    year_cols = []
    for c in df.columns:
        s = str(c).lower()
        if ("export" in s) and ("value" in s) and any(str(y) in s for y in range(2013, 2025)):
            year_cols.append(c)
    return year_cols

ITALY_CODE  = find_col(italy_df.columns, ["product", "code"]) or "Product code"
ITALY_LABEL = find_col(italy_df.columns, ["product", "label"]) or "Product label"
italy_year_cols = guess_year_value_cols(italy_df)

WORLD_CODE  = find_col(world_df.columns, ["code"]) or "Code"
WORLD_LABEL = find_col(world_df.columns, ["product", "label"]) or "Product label"
world_year_cols = guess_year_value_cols(world_df)

print("ITALY_CODE:", ITALY_CODE)
print("ITALY_LABEL:", ITALY_LABEL)
print("Italy year columns:", len(italy_year_cols))
print("WORLD_CODE:", WORLD_CODE)
print("WORLD_LABEL:", WORLD_LABEL)
print("World year columns:", len(world_year_cols))

if len(italy_year_cols) == 0 or len(world_year_cols) == 0:
    raise ValueError("Year export-value columns not detected. Check file columns.")

# ===============================
# 5) Wide -> Long
# ===============================
def to_long(df: pd.DataFrame, code_col: str, label_col: str, value_cols: list[str]) -> pd.DataFrame:
    out = df.melt(
        id_vars=[code_col, label_col],
        value_vars=value_cols,
        var_name="year_raw",
        value_name="value"
    )
    out["year"] = out["year_raw"].astype(str).str.extract(r"(\d{4})").astype(int)
    out["hs6"] = (
        out[code_col]
        .astype(str)
        .str.replace(r"\D", "", regex=True)
        .str.zfill(6)
    )
    out["value"] = pd.to_numeric(out["value"], errors="coerce").fillna(0.0)
    out = out.rename(columns={label_col: "product_label"})
    return out[["year", "hs6", "product_label", "value"]]

italy_long = to_long(italy_df, ITALY_CODE, ITALY_LABEL, italy_year_cols)
world_long = to_long(world_df, WORLD_CODE, WORLD_LABEL, world_year_cols)

# حذف ردیف‌های کل
italy_long = italy_long[italy_long["hs6"] != "000000"].copy()
world_long = world_long[world_long["hs6"] != "000000"].copy()

# ===============================
# 6) Aggregate (year-hs6)
# ===============================
italy_agg = italy_long.groupby(["year", "hs6"], as_index=False).agg(
    product_label=("product_label", "first"),
    x_italy=("value", "sum")
)
world_agg = world_long.groupby(["year", "hs6"], as_index=False).agg(
    x_world=("value", "sum")
)

# totals per year
X_italy = italy_agg.groupby("year", as_index=False)["x_italy"].sum().rename(columns={"x_italy":"X_italy"})
X_world = world_agg.groupby("year", as_index=False)["x_world"].sum().rename(columns={"x_world":"X_world"})

df = (
    italy_agg
    .merge(world_agg, on=["year","hs6"], how="left")
    .merge(X_italy, on="year")
    .merge(X_world, on="year")
)
df["x_world"] = df["x_world"].fillna(0.0)

# ===============================
# 7) RCA + RSCA (متقارن)
# ===============================
eps = 1e-12
share_i = (df["x_italy"] + eps) / (df["X_italy"] + eps)
share_w = (df["x_world"] + eps) / (df["X_world"] + eps)

df["RCA"] = share_i / share_w
df["RSCA"] = (df["RCA"] - 1) / (df["RCA"] + 1)

# ===============================
# 8) Filter RSCA = 0.8 / 0.9 / 1.0 (rounded to 1 decimal)
# ===============================
df["RSCA_1d"] = df["RSCA"].round(1)
selected = df[df["RSCA_1d"].isin([0.8, 0.9, 1.0])].copy()

# ===============================
# 9) Save outputs in the same folder
# ===============================
out_full = os.path.join(BASE_DIR, "italy_hs6_rca_rsca_2013_2024.csv")
out_sel  = os.path.join(BASE_DIR, "italy_hs6_selected_rsca_0p8_0p9_1p0.csv")

df.to_csv(out_full, index=False)
selected.to_csv(out_sel, index=False)

print("DONE ✔")
print("Full rows:", len(df))
print("Selected rows:", len(selected))
print("Saved:", out_full)
print("Saved:", out_sel)

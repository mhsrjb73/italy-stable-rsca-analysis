import pandas as pd
import os

print("EU_PARTNER_COVERAGE_ANALYSIS = START")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ===============================
# 1) Load stable RSCA HS6 list
# ===============================
stable = pd.read_csv(
    os.path.join(BASE_DIR, "italy_hs6_stable_min3years_avg_rsca.csv")
)

stable_hs6 = set(stable["hs6"].astype(str))

print("Stable HS6 count:", len(stable_hs6))

# ===============================
# 2) Partner export files
# ===============================
partner_files = {
    "Germany": "export_italy_germany.csv",
    "France": "export_italy_france.csv",
    "Spain": "export_italy_spain.csv",
    "Netherlands": "export_italy_netherlands.csv",
    "Belgium": "export_italy_belgium.csv",
    "Poland": "export_italy_poland.csv",
    "Austria": "export_italy_austria.csv",
    "Sweden": "export_italy_sweden.csv",
    "Czechia": "export_italy_czechia.csv",
    "Romania": "export_italy_romania.csv"
}

results = []

# ===============================
# 3) Coverage calculation
# ===============================
for country, fname in partner_files.items():
    path = os.path.join(BASE_DIR, fname)
    df = pd.read_csv(path)

    # تشخیص ستون کد تعرفه
    hs_col = None
    for c in df.columns:
        if "hs" in c.lower() or "code" in c.lower():
            hs_col = c
            break

    if hs_col is None:
        raise ValueError(f"HS code column not found in {fname}")

    df[hs_col] = df[hs_col].astype(str).str.replace(r"\D", "", regex=True).str.zfill(6)

    exported_hs6 = set(df[hs_col])

    covered = stable_hs6.intersection(exported_hs6)

    results.append({
        "partner": country,
        "stable_hs6_total": len(stable_hs6),
        "stable_hs6_exported": len(covered),
        "coverage_ratio": len(covered) / len(stable_hs6)
    })

# ===============================
# 4) Save results
# ===============================
coverage_df = pd.DataFrame(results).sort_values("coverage_ratio", ascending=False)

coverage_df.to_csv(
    os.path.join(BASE_DIR, "italy_stable_rsca_partner_coverage.csv"),
    index=False
)

print("DONE ✔")
print(coverage_df)

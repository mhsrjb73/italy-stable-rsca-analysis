import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

print("MAKE_FIGURES = START")

BASE_DIR = os.path.expanduser("~/Downloads/italy")

# Inputs (from your steps)
STEP1_FILE = os.path.join(BASE_DIR, "step1_partner_value_share_stable.csv")
STEP2_FREQ_FILE = os.path.join(BASE_DIR, "step2_hs6_partner_frequency.csv")
STEP3_FILE = os.path.join(BASE_DIR, "step3_partner_weighted_rsca_coverage.csv")
STEP4_FILE = os.path.join(BASE_DIR, "step4_partner_clusters.csv")
CENTROIDS_FILE = os.path.join(BASE_DIR, "step4_cluster_centroids.csv")

# Output dir for figures
FIG_DIR = os.path.join(BASE_DIR, "figures")
os.makedirs(FIG_DIR, exist_ok=True)

def save_fig(name: str):
    path = os.path.join(FIG_DIR, name)
    plt.tight_layout()
    plt.savefig(path, dpi=220)
    plt.close()
    print("Saved figure:", path)

def safe_read_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing file: {path}")
    return pd.read_csv(path)

# ---------------------------
# Figure 1: Stable vs Total (if total not available, show stable count only)
# ---------------------------
stable = None
# try to find stable file name you used earlier
stable_candidates = [
    os.path.join(BASE_DIR, "italy_hs6_stable_min3years_avg_rsca.csv"),
    os.path.join(BASE_DIR, "italy_hs6_stable_min3years_avg_rsca.csv".lower()),
]
for c in stable_candidates:
    if os.path.exists(c):
        stable = safe_read_csv(c)
        break

stable_count = int(stable["hs6"].astype(str).str.replace(r"\D","", regex=True).str.zfill(6).nunique()) if stable is not None else 2448

# If you want total HS6 count too, you can optionally read the full RSCA file produced earlier
total_candidates = [
    os.path.join(BASE_DIR, "italy_hs6_rca_rsca_2013_2024.csv"),
    os.path.join(BASE_DIR, "italy_hs6_rca_rsca_2013_2024.csv".lower()),
]
total_count = None
for c in total_candidates:
    if os.path.exists(c):
        full = safe_read_csv(c)
        # try common columns
        hs_col = "hs6" if "hs6" in full.columns else ("HS6" if "HS6" in full.columns else None)
        if hs_col:
            total_count = int(full[hs_col].astype(str).str.replace(r"\D","", regex=True).str.zfill(6).nunique())
        break

plt.figure()
labels = ["Stable HS6"]
values = [stable_count]
if total_count is not None and total_count >= stable_count:
    labels = ["Stable HS6", "All HS6 (in dataset)"]
    values = [stable_count, total_count]
plt.bar(labels, values)
plt.ylabel("Count of HS6 codes")
plt.title("Stable comparative advantage set size (HS6)")
save_fig("fig1_stable_vs_total_hs6.png")

# ---------------------------
# Figure 2: Partner coverage distribution (Histogram of partner_count)
# ---------------------------
freq = safe_read_csv(STEP2_FREQ_FILE)
if "partner_count" not in freq.columns:
    raise ValueError("step2_hs6_partner_frequency.csv must have 'partner_count'")

plt.figure()
bins = np.arange(0.5, 10.6, 1.0)
plt.hist(freq["partner_count"], bins=bins, edgecolor="black")
plt.xticks(range(1, 11))
plt.xlabel("Number of partners where HS6 is exported (value>0 in any year)")
plt.ylabel("Number of stable HS6 codes")
plt.title("Geographic scalability of Italy's stable advantages (2013–2024)")
save_fig("fig2_partner_count_histogram.png")

# Also produce the key thresholds bar (>=3, >=5, all10)
ge3 = int((freq["partner_count"] >= 3).sum())
ge5 = int((freq["partner_count"] >= 5).sum())
all10 = int((freq["partner_count"] == 10).sum())

plt.figure()
plt.bar([">=3 partners", ">=5 partners", "All 10 partners"], [ge3, ge5, all10])
plt.ylabel("Count of HS6 codes")
plt.title("Stable HS6 exported across partners (threshold view)")
save_fig("fig2b_partner_thresholds.png")

# ---------------------------
# Figure 3: Value share of stable advantages by partner (Step 1)
# ---------------------------
s1 = safe_read_csv(STEP1_FILE)
needed = {"partner", "value_share_stable"}
if not needed.issubset(set(s1.columns)):
    raise ValueError(f"{STEP1_FILE} must contain columns: {needed}")

s1p = s1.copy()
s1p = s1p.sort_values("value_share_stable", ascending=False)

plt.figure(figsize=(9, 4.8))
plt.bar(s1p["partner"], s1p["value_share_stable"])
plt.xticks(rotation=35, ha="right")
plt.ylabel("Share of export value from stable-advantage HS6")
plt.title("Value share of stable advantages in exports to each partner (2013–2024)")
save_fig("fig3_value_share_stable_by_partner.png")

# ---------------------------
# Figure 4: Weighted RSCA coverage by partner (Step 3)
# ---------------------------
s3 = safe_read_csv(STEP3_FILE)
needed = {"partner", "weighted_rsca_coverage"}
if not needed.issubset(set(s3.columns)):
    raise ValueError(f"{STEP3_FILE} must contain columns: {needed}")

s3p = s3.sort_values("weighted_rsca_coverage", ascending=False)

plt.figure(figsize=(9, 4.8))
plt.bar(s3p["partner"], s3p["weighted_rsca_coverage"])
plt.xticks(rotation=35, ha="right")
plt.ylabel("Weighted RSCA coverage")
plt.title("Absorption of Italy's strongest stable advantages (RSCA-weighted)")
save_fig("fig4_weighted_rsca_coverage_by_partner.png")

# ---------------------------
# Figure 5: Synthetic scatter (Value share vs Weighted RSCA), colored by cluster (Step 4)
# ---------------------------
cl = safe_read_csv(STEP4_FILE)
needed = {"partner", "value_share_stable", "weighted_rsca_coverage", "cluster"}
if not needed.issubset(set(cl.columns)):
    raise ValueError(f"{STEP4_FILE} must contain columns: {needed}")

plt.figure(figsize=(7.2, 5.4))
clusters = sorted(cl["cluster"].unique())
for k in clusters:
    sub = cl[cl["cluster"] == k]
    plt.scatter(sub["weighted_rsca_coverage"], sub["value_share_stable"], label=f"Cluster {k}", s=60)

for _, r in cl.iterrows():
    plt.text(r["weighted_rsca_coverage"], r["value_share_stable"], " " + str(r["partner"]), fontsize=9, va="center")

plt.xlabel("Weighted RSCA coverage")
plt.ylabel("Value share of stable advantages")
plt.title("Partner typology: value-intensity vs structural alignment")
plt.legend()
save_fig("fig5_scatter_value_vs_weighted_rsca_clusters.png")

# Optional: show cluster centroids plot
if os.path.exists(CENTROIDS_FILE):
    cen = safe_read_csv(CENTROIDS_FILE)
    if {"cluster","value_share_stable","weighted_rsca_coverage"}.issubset(cen.columns):
        plt.figure(figsize=(7.2, 5.4))
        for _, r in cen.iterrows():
            plt.scatter(r["weighted_rsca_coverage"], r["value_share_stable"], s=150)
            plt.text(r["weighted_rsca_coverage"], r["value_share_stable"], f"  centroid {int(r['cluster'])}", fontsize=10, va="center")
        plt.xlabel("Weighted RSCA coverage")
        plt.ylabel("Value share of stable advantages")
        plt.title("Cluster centroids (original scale)")
        save_fig("fig5b_cluster_centroids.png")

print("DONE ✔")
print("All figures are in:", FIG_DIR)


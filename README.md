# Italy Stable Comparative Advantage Analysis (HS6)

This project analyzes Italy’s export structure using a stability-adjusted revealed comparative advantage (RSCA) framework at the HS6 product level.

## Objective
To identify Italy’s **persistent comparative advantage core** and assess how consistently these products are absorbed by major European trading partners.

## Data
- Source: Trade Map (International Trade Centre — ITC)
- Period: 2013–2024
- Level: HS6 product classification
- Partners: Germany, France, Spain, Switzerland, Poland, Belgium, Netherlands, Austria, Romania, Czech Republic

## Methodology
1. Compute RCA and symmetric RSCA for all HS6 products.
2. Define a **stable advantage core** (RSCA > 0 in ≥3 years).
3. Measure partner coverage using export presence and export value.
4. Calculate value shares and weighted RSCA coverage.
5. Cluster partner countries based on structural absorption patterns.

## Outputs
- Stable HS6 product set
- Partner coverage indicators
- Value-based competitiveness metrics
- Visual analytics and clustering results

## Tools
- Python
- pandas, numpy
- matplotlib
- scikit-learn

## Author
Mahsa Rajabi Nejad  
Author’s calculations based on ITC Trade Map data.

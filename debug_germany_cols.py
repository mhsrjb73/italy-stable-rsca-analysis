import os
import pandas as pd
from io import StringIO

BASE_DIR = os.path.expanduser("~/Downloads/italy")
FILE = os.path.join(BASE_DIR, "italy to germany.xls")

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
    raise RuntimeError("Cannot parse html")

df = read_trademap_html(FILE)

print("SHAPE:", df.shape)
print("\nCOLUMNS:")
for i, c in enumerate(df.columns):
    print(i, repr(c))

print("\nHEAD (first 5 rows):")
print(df.head(5).to_string())


# peek_csv.py
import pandas as pd
df = pd.read_csv("data/matches.csv")
print(df.columns.tolist())
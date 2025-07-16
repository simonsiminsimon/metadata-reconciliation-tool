import pandas as pd
df = pd.read_csv('test_entities.csv')
print(f"Rows: {len(df)}")
print(f"Columns: {list(df.columns)}")
print(f"Creator names sample: {df['creator_name'].head().tolist()}")
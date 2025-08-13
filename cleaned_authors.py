import pandas as pd

INPUT_PATH = "/home/emcj/data/MAG/AuthorsGenderized_merged.csv"
OUTPUT_PATH = "/home/emcj/data/MAG/AuthorsGenderized_merged_clean.csv"

df = pd.read_csv(INPUT_PATH, on_bad_lines='skip')
print("read file")
df = df[df['Gender'].str.lower() != 'unknown']
print("dropped columns")
df_cleaned = df.drop_duplicates(subset="DisplayName", keep="first")
print("dropped duplicates")
df_cleaned.to_csv(OUTPUT_PATH, index=False)



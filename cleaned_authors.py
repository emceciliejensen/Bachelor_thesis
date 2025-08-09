import pandas as pd

INPUT_PATH = "/home/emcj/data/MAG/AuthorsGenderized_merged.csv"
OUTPUT_PATH = "/home/emcj/data/MAG/AuthorsGenderized_merged_clean.csv"

# Load the data while skipping malformed lines
df = pd.read_csv(INPUT_PATH, on_bad_lines='skip')
print("read file")
# Remove rows where gender is unknown (case insensitive)
df = df[df['Gender'].str.lower() != 'unknown']
print("dropped columns")
# Drop duplicates based on Name (keep first)
df_cleaned = df.drop_duplicates(subset="DisplayName", keep="first")
print("dropped duplicates")
# Save cleaned file
df_cleaned.to_csv(OUTPUT_PATH, index=False)

print(f"Cleaned data saved to: {OUTPUT_PATH}")
print(f"Original rows (after skipping bad lines): {len(df)}")
print(f"Rows after removing unknown genders and duplicates: {len(df_cleaned)}")


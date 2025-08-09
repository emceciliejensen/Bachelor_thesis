import polars as pl

INPUT_PATH = "/home/emcj/data/MAG/AuthorsGenderized_merged.csv"
FILTERED_PATH = "/home/emcj/data/MAG/AuthorsGenderized_filtered.csv"
OUTPUT_PATH = "/home/emcj/data/MAG/AuthorsGenderized_clean.csv"

# Step 1: Filter rows (exclude unknown genders) and write to temp file
print("Filtering...")
df = pl.read_csv(INPUT_PATH, ignore_errors=True, low_memory=True).lazy()
df_filtered = df.filter(pl.col("Gender").str.to_lowercase() != "unknown")
df_filtered.collect(streaming=True).write_csv(FILTERED_PATH)

# Step 2: Sort and remove duplicates by DisplayName
print("Sorting and deduplicating...")
df_final = (
    pl.read_csv(FILTERED_PATH)
    .sort("DisplayName")  # sort required before deduplication
    .unique(subset=["DisplayName"], keep="first")
)
df_final.write_csv(OUTPUT_PATH)

print("Finished writing cleaned CSV:", OUTPUT_PATH)


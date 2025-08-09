import polars as pl

INPUT = "/home/emcj/data/MAG/AuthorsGenderized_merged.csv"
FILTERED = "/home/emcj/data/MAG/AuthorsGenderized_unknown_filter.csv"

df = pl.read_csv(INPUT)
filtered = df.filter(pl.col("Gender").str.to_lowercase() != "unknown")
filtered.write_csv(FILTERED)


import polars as pl

df = pl.read_csv("/home/emcj/data/MAG/Medicine_Author_Citations_Genderized.csv")

#Citation count for target author.
citation_counts = (
    df.group_by("TargetAuthorId")
      .agg(pl.len().alias("CitationCount"))
      .rename({"TargetAuthorId": "AuthorId"})
)

#getting gender info.
source_gender = (
    df.select(["SourceAuthorId", "SourceGender"])
      .rename({"SourceAuthorId": "AuthorId", "SourceGender": "Gender"})
      .unique()
)

target_gender = (
    df.select(["TargetAuthorId", "TargetGender"])
      .rename({"TargetAuthorId": "AuthorId", "TargetGender": "Gender"})
      .unique()
)

#Combining gender info and deduplicate.
gender_df = pl.concat([source_gender, target_gender]).unique(subset=["AuthorId"])

#Merging with gender.
result = (
    citation_counts
    .join(gender_df, on="AuthorId", how="left")
    .with_columns([
        pl.col("Gender").fill_null("unknown")
    ])
)

#Saving to csv.
result.write_csv("/home/emcj/data/MAG/Medicine_citation_count.csv")
print("Medicine")

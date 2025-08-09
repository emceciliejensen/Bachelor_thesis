import polars as pl

df = pl.read_csv("/home/emcj/data/MAG/Medicine_Author_Citations_Genderized.csv")

#Counting unique citing source authors for each target author.
unique_citers = (
    df.group_by("TargetAuthorId")
      .agg(pl.col("SourceAuthorId").n_unique().alias("UniqueCitingAuthors"))
      .rename({"TargetAuthorId": "AuthorId"})
)

#Getting gender info.
source_gender = df.select(["SourceAuthorId", "SourceGender"]).rename({
    "SourceAuthorId": "AuthorId", "SourceGender": "Gender"
}).unique()

target_gender = df.select(["TargetAuthorId", "TargetGender"]).rename({
    "TargetAuthorId": "AuthorId", "TargetGender": "Gender"
}).unique()

#Combining gender info and deduplicate.
gender_df = pl.concat([source_gender, target_gender]).unique(subset=["AuthorId"])

#Merging with gender.
result = unique_citers.join(gender_df, on="AuthorId", how="left")
result = result.with_columns([
    pl.col("Gender").fill_null("unknown")
])

#Saving to csv.
result.write_csv("/home/emcj/data/MAG/Medicine_unique_citing_authors.csv")
print("Medicine")


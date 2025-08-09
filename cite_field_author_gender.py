import polars as pl

def debug_check(file_path):
    print(f"\n[DEBUG] Reading first few rows from: {file_path}")
    try:
        df_preview = pl.read_csv(file_path, n_rows=5)
        print("[DEBUG] Columns found:", df_preview.columns)
    except Exception as e:
        print("[ERROR] Failed to read file:", e)
        raise

def add_gender_to_edges(author_citations_file, author_gender_file, output_file):
    debug_check(author_gender_file)
    debug_check(author_citations_file)

    # Read gender data as LazyFrame
    gender_df = pl.read_csv(
        author_gender_file,
        infer_schema_length=0
    ).with_columns(
        pl.col("AuthorId").cast(pl.Int64)
    ).select(["AuthorId", "Gender", "Genderized"]).lazy()

# Read citation edges as LazyFrame
    edges = pl.read_csv(author_citations_file).lazy()

# Join source
    gender_source = gender_df.rename({"AuthorId": "AuthorId_source"})

    edges = edges.join(
        gender_source,
        left_on="SourceAuthorId",
        right_on="AuthorId_source",
        how="left",
#        stream=True
    ).rename({
        "Gender": "SourceGender",
        "Genderized": "SourceGenderized"
    })

# Join target
    gender_target = gender_df.rename({"AuthorId": "AuthorId_target"})

    edges = edges.join(
        gender_target,
        left_on="TargetAuthorId",
        right_on="AuthorId_target",
        how="left",
#        stream=True
    ).rename({
        "Gender": "TargetGender",
        "Genderized": "TargetGenderized"
    })

# Select columns and collect result
    edges = edges.select([
        "SourceAuthorId", "SourceGender", "SourceGenderized",
        "TargetAuthorId", "TargetGender", "TargetGenderized"
    ]).collect()  # Only here you collect it into memory

# Write to CSV
    edges.write_csv(output_file)



    # Read gender data (keep AuthorId as int)
#    gender_df = pl.read_csv(
#    	author_gender_file,
#    	infer_schema_length=0
#    ).with_columns([
#    	pl.col("AuthorId").cast(pl.Int64)
#    ]).select(["AuthorId", "Gender", "Genderized"])

    # Read citation edges (IDs already int64)
#    edges = pl.read_csv(author_citations_file)

    # Rename for source author
# Force the renamed frame to materialize
#    gender_source = gender_df.rename({"AuthorId": "AuthorId_source"})

#    edges = edges.join(
#    	gender_source,
#    	left_on="SourceAuthorId",
#    	right_on="AuthorId_source",
#    	how="left"
#    ).rename({
#    	"Gender": "SourceGender",
#    	"Genderized": "SourceGenderized"
#    })

#    gender_target = gender_df.rename({"AuthorId": "AuthorId_target"})

#    edges = edges.join(
#        gender_target,
#        left_on="TargetAuthorId",
#        right_on="AuthorId_target",
#        how="left"
#    ).rename({
#        "Gender": "TargetGender",
#        "Genderized": "TargetGenderized"
#    })

    # Merge source gender info
    #edges = edges.join(
    #    gender_df,
    #    left_on="SourceAuthorId",
    #    right_on="AuthorId",
    #    how="left"
    #).rename({
    #    "Gender": "SourceGender",
    #    "Genderized": "SourceGenderized"
    #}).drop("AuthorId_source")

    # Merge target gender info
    #edges = edges.join(
    #    gender_df,
    #    left_on="TargetAuthorId",
    #    right_on="AuthorId",
    #    how="left"
    #).rename({
    #    "Gender": "TargetGender",
    #    "Genderized": "TargetGenderized"
    #}).drop("AuthorId_target")

#    edges = edges.select([
#    	"SourceAuthorId", "SourceGender", "SourceGenderized",
#    	"TargetAuthorId", "TargetGender", "TargetGenderized"
#    ])

#    edges.write_csv(output_file)
#    print(f"[✅] Done. Output written to: {output_file}")

# Example usage
if __name__ == "__main__":
    add_gender_to_edges(
        "/home/emcj/data/MAG/Physics_Author_Citations_top_1000.csv",
        "/home/emcj/data/MAG/Cleaned_Authors_Genderized.csv",
        "/home/emcj/data/MAG/Physics_Author_Citations_top_1000_Genderized.csv"
    )


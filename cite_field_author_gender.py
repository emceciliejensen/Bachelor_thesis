import polars as pl


def add_gender_to_edges(author_citations_file, author_gender_file, output_file):

    #Reading data
    gender_df = pl.read_csv(
        author_gender_file,
        infer_schema_length=0
    ).with_columns(
        pl.col("AuthorId").cast(pl.Int64)
    ).select(["AuthorId", "Gender", "Genderized"]).lazy()

#Reading citation edges 
    edges = pl.read_csv(author_citations_file).lazy()

#Joining with source
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

#Joining with target
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

#Selecting columns
    edges = edges.select([
        "SourceAuthorId", "SourceGender", "SourceGenderized",
        "TargetAuthorId", "TargetGender", "TargetGenderized"

#Writing to CSV
    edges.write_csv(output_file)


#Running function
if __name__ == "__main__":
    add_gender_to_edges(
        "/home/emcj/data/MAG/Engineering_Author_Citations.csv",
        "/home/emcj/data/MAG/Cleaned_Authors_Genderized.csv",
        "/home/emcj/data/MAG/Engineering_Author_Citations_Genderized.csv"
    )


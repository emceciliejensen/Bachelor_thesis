import polars as pl
from polars import read_csv_batched

def debug_check(file_path):
    print(f"[DEBUG] Checking file: {file_path}")
    try:
        df = pl.read_csv(file_path, n_rows=5)
        print("[DEBUG] Columns found:", df.columns)
    except Exception as e:
        print("[ERROR] Could not read file:", e)
        raise

def add_gender_to_edges(author_citations_file, author_gender_file, output_file):
    print("[INFO] Script started", flush=True)

    debug_check(author_gender_file)
    debug_check(author_citations_file)

    # Read gender CSV as LazyFrame
    gender_df = (
        pl.read_csv(author_gender_file, infer_schema_length=0)
        .with_columns(pl.col("AuthorId").cast(pl.Int64))
        .select(["AuthorId", "Gender", "Genderized"])
        .lazy()
    )

    reader = read_csv_batched(author_citations_file, batch_size=5_000_000)
    first_chunk = True
    batch_num = 0

    while True:
        batches = reader.next_batches(1)
        if not batches:
            break

        chunk = batches[0]
        print(f"[INFO] Processing batch #{batch_num}", flush=True)
        batch_num += 1

        # Join with source gender
        gender_source = gender_df.rename({"AuthorId": "AuthorId_source"})
        chunk_lazy = chunk.lazy().join(
            gender_source,
            left_on="SourceAuthorId",
            right_on="AuthorId_source",
            how="left"
        ).rename({
            "Gender": "SourceGender",
            "Genderized": "SourceGenderized"
        })

        # Join with target gender
        gender_target = gender_df.rename({"AuthorId": "AuthorId_target"})
        chunk_lazy = chunk_lazy.join(
            gender_target,
            left_on="TargetAuthorId",
            right_on="AuthorId_target",
            how="left"
        ).rename({
            "Gender": "TargetGender",
            "Genderized": "TargetGenderized"
        })

        # Select desired columns and collect
        result = chunk_lazy.select([
            "SourceAuthorId", "SourceGender", "SourceGenderized",
            "TargetAuthorId", "TargetGender", "TargetGenderized"
        ]).collect()

        # Write to file
        if first_chunk:
            result.write_csv(output_file)
            first_chunk = False
        else:
            with open(output_file, "a") as f:
                result.write_csv(file=f, include_header=False)

    print(f"[✓] Done. Output written to: {output_file}")

# Run the function
if __name__ == "__main__":
    add_gender_to_edges(
        "/home/emcj/data/MAG/Medicine_Author_Citations.csv",
        "/home/emcj/data/MAG/Cleaned_Authors_Genderized.csv",
        "/home/emcj/data/MAG/Medicine_Author_Citations_Genderized.csv"
    )


import pandas as pd

# Limit for number of matches to keep
MAX_MATCHES = 1000

# Your existing setup
PFOS_PATH = "/home/emcj/data/MAG/PaperFieldsOfStudy.csv"
PAA_PATH = "/home/emcj/data/MAG/PaperAuthorAffiliations.csv"
field_ids = {
    "physics": 121332964,
}

df_PFOS = pd.read_csv(PFOS_PATH)

for field_name, field_id in [("physics", 121332964)]:
    print(f"\nProcessing field: {field_name} (ID={field_id})")
    
    # Filter papers in this field
    field_papers = df_PFOS[df_PFOS["FieldOfStudyId"] == field_id]
    paper_ids = set(field_papers["PaperId"].head(1000)) 
#    paper_ids = set(field_papers["PaperId"])
    print(f"Found {len(paper_ids):,} papers in this field")

    matched_rows = []
    total_matches = 0
    chunksize = 100

    print(f"Reading {PAA_PATH} in chunks...")

    for chunk in pd.read_csv(PAA_PATH, chunksize=chunksize):
        filtered = chunk[chunk["PaperId"].isin(paper_ids)][["PaperId", "AuthorId"]]
        matched_rows.append(filtered)
        total_matches += len(filtered)
        print(f"  Matched {len(filtered):,} rows (total: {total_matches:,})")

        # Stop early if limit reached
        if total_matches >= MAX_MATCHES:
            print(f"Reached limit of {MAX_MATCHES:,} rows, stopping early.")
            break

    if matched_rows:
        df_authors = pd.concat(matched_rows, ignore_index=True)
        df_authors["FieldOfStudyId"] = field_id
        df_authors = df_authors[["FieldOfStudyId", "PaperId", "AuthorId"]]

        outname = f"{field_name}_papers_with_authors_limited.csv"
        df_authors.to_csv(outname, index=False)
        print(f"Saved {len(df_authors):,} rows to {outname}")
    else:
        print("No matches found.")


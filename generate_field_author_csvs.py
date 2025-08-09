import pandas as pd

# Load the big datasets
print("Loading datasets...")
df_PFOS = pd.read_csv("/home/emcj/data/MAG/PaperFieldsOfStudy.csv")
df_PAA = pd.read_csv("/home/emcj/data/MAG/PaperAuthorAffiliations.csv")

# Define the field IDs of interest
field_ids = {
    "medicine": 71924100,
    "sociology": 144024400,
    "engineering": 127413603,
    "psychology": 15744967,
    "physics": 121332964,
}

# Process each field
for field_name, field_id in field_ids.items():
    print(f"Processing {field_name}...")
    
    # Filter to just papers in that field
    field_papers = df_PFOS[df_PFOS["FieldOfStudyId"] == field_id]

    # Join with authors on PaperId
    merged = pd.merge(field_papers, df_PAA, on="PaperId", how="inner")

    # Select columns
    output_df = merged[["FieldOfStudyId", "PaperId", "AuthorId"]]

    # Save CSV
    output_filename = f"{field_name}_papers_with_authors.csv"
    output_df.to_csv(output_filename, index=False)
    
    print(f"Saved {len(output_df)} rows to {output_filename}")


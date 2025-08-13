import pandas as pd

print("Loading datasets...")
df_PFOS = pd.read_csv("/home/emcj/data/MAG/PaperFieldsOfStudy.csv")
df_PAA = pd.read_csv("/home/emcj/data/MAG/PaperAuthorAffiliations.csv")

field_ids = {
    "medicine": 71924100,
    "sociology": 144024400,
    "engineering": 127413603,
    "psychology": 15744967,
    "physics": 121332964,
}

for field_name, field_id in field_ids.items():
    print(f"Processing {field_name}...")
    
    field_papers = df_PFOS[df_PFOS["FieldOfStudyId"] == field_id]

    merged = pd.merge(field_papers, df_PAA, on="PaperId", how="inner")

    output_df = merged[["FieldOfStudyId", "PaperId", "AuthorId"]]

    output_filename = f"{field_name}_papers_with_authors.csv"
    output_df.to_csv(output_filename, index=False)
    


import pandas as pd

# Load CSV
df_paper_ref = pd.read_csv("/home/emcj/data/MAG/PaperReferences.csv")

# Rename columns
df_paper_ref.columns = ['PaperId', 'PaperReferenceId']

# Save to new CSV
df_paper_ref.to_csv("/home/emcj/data/MAG/PaperReferences.csv", index=False)

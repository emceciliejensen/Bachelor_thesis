import pandas as pd

df_paper_ref = pd.read_csv("/home/emcj/data/MAG/PaperReferences.csv")

df_paper_ref.columns = ['PaperId', 'PaperReferenceId']

df_paper_ref.to_csv("/home/emcj/data/MAG/PaperReferences.csv", index=False)

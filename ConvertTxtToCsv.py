import pandas as pd

columns = [
	'PaperId:long', 'FieldOfStudyId:long', 'Score:float'
]

chunk_size = 100000 

reader = pd.read_csv(
    "/home/emcj/data/MAG/PaperFieldsOfStudy.txt",
    delimiter="\t",
    names=columns,
    header=None,
    dtype=str,
    on_bad_lines='skip',
    chunksize=chunk_size
)
with open("PaperFieldsOfStudy.csv", "w") as f:
    for i, chunk in enumerate(reader):
        chunk.to_csv(f, index=False, header=(i == 0))








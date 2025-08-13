import pandas as pd

FIELD_ID = 71924100  # medicine
INPUT_PATH = "/home/emcj/data/MAG/PaperFieldsOfStudy.csv"
OUTPUT_PATH = "/home/emcj/data/MAG/medicine_PFOS.csv"
CHUNKSIZE = 250_000

first_chunk = True
total_rows = 0
chunk_num = 0

for chunk in pd.read_csv(INPUT_PATH, chunksize=CHUNKSIZE):
    chunk_num += 1
    filtered = chunk[chunk["FieldOfStudyId"] == FIELD_ID]
    
    if not filtered.empty:
        filtered.to_csv(OUTPUT_PATH, mode='a', index=False, header=first_chunk)
        total_rows += len(filtered)
        print(f"Chunk {chunk_num}: wrote {len(filtered)} rows (total so far: {total_rows})")
        first_chunk = False



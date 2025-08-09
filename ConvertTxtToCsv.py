import pandas as pd

columns = [
	'PaperId:long', 'FieldOfStudyId:long', 'Score:float'
]

chunk_size = 100000  # adjust if needed

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
       	# Convert datetime columns
       # for col in ['Date:DateTime?', 'OnlineDate:DateTime?', 'CreatedDate:DateTime']:
        #	   chunk[col] = pd.to_datetime(chunk[col], errors='coerce')

        # Convert numeric columns with possible missing values
       # numeric_cols = [
         #   'Rank:uint', 'Year:int?', 'JournalId:long?', 'ConferenceSeriesId:long?',
          #  'ConferenceInstanceId:long?', 'ReferenceCount:long', 'CitationCount:long',
           # 'EstimatedCitation:long', 'FamilyId:long?', 'FamilyRank:uint?'
       # ]
       # for col in numeric_cols:
           # chunk[col] = pd.to_numeric(chunk[col], errors='coerce')

        # Write chunk to CSV
        chunk.to_csv(f, index=False, header=(i == 0))








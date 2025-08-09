import pandas as pd
from collections import defaultdict

citations_iter = pd.read_csv("/home/emcj/data/MAG/Physics_Author_Citations_random_sample_Genderized.csv", chunksize=1_000_000)
papers = pd.read_csv("/home/emcj/data/MAG/Physics_Author_Papers.csv")

author_paper_map = defaultdict(list)

#Processing in chunks.
for chunk in citations_iter:
    chunk = chunk.rename(columns={"TargetAuthorId": "AuthorId"})
    merged = chunk.merge(papers, on="AuthorId", how="inner")

    paper_counts = merged.groupby("PaperId").size().reset_index(name="CitationCount")
    author_papers = papers.merge(paper_counts, on="PaperId", how="left").fillna(0)

    for row in author_papers.itertuples(index=False):
        author_paper_map[row.AuthorId].append(int(row.CitationCount))

#Computing H-index.
def h_index(citations):
    citations.sort(reverse=True)
    return sum((i + 1) <= c for i, c in enumerate(citations))

h_index_data = [{"AuthorId": aid, "HIndex": h_index(cites)} for aid, cites in author_paper_map.items()]
h_index_df = pd.DataFrame(h_index_data)

#Merging with gender info.
gender_df = pd.read_csv("/home/emcj/data/MAG/Physics_Author_Citations_random_sample_Genderized.csv", usecols=["TargetAuthorId", "TargetGender", "TargetGenderized"]).drop_duplicates()
gender_df = gender_df.rename(columns={
    "TargetAuthorId": "AuthorId",
    "TargetGender": "Gender",
    "TargetGenderized": "Genderized"
})

#Final merging and saving as a csv.
final_df = h_index_df.merge(gender_df, on="AuthorId", how="left")
final_df["Gender"] = final_df["Gender"].fillna("unknown")

final_df.to_csv("/home/emcj/data/MAG/Physics_random_sample_H_index_with_gender.csv", index=False)
print("H-index Physics random sample.")

import pandas as pd
from collections import defaultdict

citations_iter = pd.read_csv("/home/emcj/data/MAG/Physics_Author_Citations_top_1000_Genderized.csv", chunksize=5_000_000)
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

#Computing i10-index.
def i10_index(citations):
    return sum(c >= 10 for c in citations)

i10_index_data = [{"AuthorId": aid, "i10index": i10_index(cites)} for aid, cites in author_paper_map.items()]
i10_index_df = pd.DataFrame(i10_index_data)

#Merging with gender info and dropping duplicates.
gender_df = pd.read_csv(
    "/home/emcj/data/MAG/Physics_Author_Citations_top_1000_Genderized.csv",
    usecols=["TargetAuthorId", "TargetGender", "TargetGenderized"]
).drop_duplicates()

gender_df = gender_df.rename(columns={
    "TargetAuthorId": "AuthorId",
    "TargetGender": "Gender",
    "TargetGenderized": "Genderized"
})

#Final merge and saving as csv.
final_df = i10_index_df.merge(gender_df, on="AuthorId", how="left")
final_df["Gender"] = final_df["Gender"].fillna("unknown")

final_df.to_csv("/home/emcj/data/MAG/Physics_top_1000_i10_index_with_gender.csv", index=False)
print("i10-index (Physics_top_1000) saved.")

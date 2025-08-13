import polars as pl
import os
import pickle
import gc
from collections import defaultdict

citations_path = "/home/emcj/data/MAG/Physics_Author_Citations_top_1000_Genderized.csv"
papers_path = "/home/emcj/data/MAG/Physics_Author_Papers.csv"
checkpoint_path = "/home/emcj/checkpoints/h_index_author_map.pkl"
output_path = "/home/emcj/data/MAG/Physics_top_1000_H_index_with_gender.csv"


papers = pl.read_csv(papers_path).select(["AuthorId", "PaperId"])

if os.path.exists(checkpoint_path) and os.path.getsize(checkpoint_path) > 0:
    with open(checkpoint_path, "rb") as f:
        author_paper_map = pickle.load(f)
else:
    author_paper_map = defaultdict(list)


citations_df = pl.read_csv(citations_path).rename({"TargetAuthorId": "AuthorId"})

batch_size = 1_000_000
num_rows = citations_df.height

for i in range(0, num_rows, batch_size):
    batch_id = i // batch_size

    chunk = citations_df.slice(i, batch_size)

    relevant_authors = chunk.select("AuthorId").unique()
    filtered_papers = papers.join(relevant_authors, on="AuthorId", how="inner")

    merged = chunk.join(filtered_papers, on="AuthorId", how="inner")

    paper_counts = (
        merged.group_by("PaperId")
              .len()
              .rename({"len": "CitationCount"})
    )

    relevant_papers = filtered_papers.join(paper_counts, on="PaperId", how="inner")

    for row in relevant_papers.iter_rows():
        aid, _, count = row
        author_paper_map[aid].append(count)

    if batch_id % 5 == 0:
        with open(checkpoint_path, "wb") as f:
            pickle.dump(author_paper_map, f)

    del chunk, relevant_authors, filtered_papers, merged, paper_counts, relevant_papers
    gc.collect()

def h_index(citations):
    citations.sort(reverse=True)
    return sum((i + 1) <= c for i, c in enumerate(citations))

h_index_data = [
    {"AuthorId": aid, "HIndex": h_index(cites)}
    for aid, cites in author_paper_map.items()
]
h_index_df = pl.DataFrame(h_index_data)

gender_df = (
    pl.read_csv(citations_path, usecols=["TargetAuthorId", "TargetGender", "TargetGenderized"])
      .unique()
      .rename({
          "TargetAuthorId": "AuthorId",
          "TargetGender": "Gender",
          "TargetGenderized": "Genderized"
      })
)

final_df = h_index_df.join(gender_df, on="AuthorId", how="left")
final_df = final_df.with_columns(
    pl.when(pl.col("Gender").is_null()).then("unknown").otherwise(pl.col("Gender")).alias("Gender")
)

final_df.write_csv(output_path)


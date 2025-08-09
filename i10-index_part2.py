import polars as pl
import os
import pickle
import gc
from collections import defaultdict

# === Paths ===
citations_path = "/home/emcj/data/MAG/Physics_Author_Citations_top_1000_Genderized.csv"
papers_path = "/home/emcj/data/MAG/Physics_Author_Papers.csv"
checkpoint_path = "/home/emcj/checkpoints/i10_author_map.pkl"
output_path = "/home/emcj/data/MAG/Physics_top_1000_i10_index_with_gender.csv"

# === Ensure checkpoint directory exists ===
os.makedirs(os.path.dirname(checkpoint_path), exist_ok=True)

print("Polars version:", pl.__version__)

# === Load paper metadata ===
papers = pl.read_csv(papers_path).select(["AuthorId", "PaperId"])

# === Restore or initialize author_paper_map ===
if os.path.exists(checkpoint_path):
    with open(checkpoint_path, "rb") as f:
        author_paper_map = pickle.load(f)
    print("✅ Loaded checkpoint.")
else:
    author_paper_map = defaultdict(list)

# === Read full citations (slice manually) ===
citations_df = pl.read_csv(citations_path).rename({"TargetAuthorId": "AuthorId"})

batch_size = 1_000_000
num_rows = citations_df.height

for i in range(0, num_rows, batch_size):
    batch_id = i // batch_size
    print(f"🔄 Processing batch {batch_id}")

    chunk = citations_df.slice(i, batch_size)

    # Filter to only relevant authors
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
        print(f"💾 Checkpoint saved after batch {batch_id}")

    del chunk, relevant_authors, filtered_papers, merged, paper_counts, relevant_papers
    gc.collect()

# === Compute i10-index ===
def i10_index(citations):
    return sum(c >= 10 for c in citations)

i10_index_data = [
    {"AuthorId": aid, "i10index": i10_index(cites)}
    for aid, cites in author_paper_map.items()
]
i10_index_df = pl.DataFrame(i10_index_data)

# === Load gender info ===
gender_df = (
    pl.read_csv(citations_path, usecols=["TargetAuthorId", "TargetGender", "TargetGenderized"])
      .unique()
      .rename({
          "TargetAuthorId": "AuthorId",
          "TargetGender": "Gender",
          "TargetGenderized": "Genderized"
      })
)

# === Merge and save ===
final_df = i10_index_df.join(gender_df, on="AuthorId", how="left")
final_df = final_df.with_columns(
    pl.when(pl.col("Gender").is_null()).then("unknown").otherwise(pl.col("Gender")).alias("Gender")
)

final_df.write_csv(output_path)
print("✅ i10-index written to:", output_path)

# === Remove checkpoint ===
if os.path.exists(checkpoint_path):
    os.remove(checkpoint_path)
    print("🧹 Checkpoint removed after success.")

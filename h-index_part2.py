import polars as pl
import os
import pickle
import gc
from collections import defaultdict

# === Paths ===
citations_path = "/home/emcj/data/MAG/Physics_Author_Citations_top_1000_Genderized.csv"
papers_path = "/home/emcj/data/MAG/Physics_Author_Papers.csv"
checkpoint_path = "/home/emcj/checkpoints/h_index_author_map.pkl"
output_path = "/home/emcj/data/MAG/Physics_top_1000_H_index_with_gender.csv"

# === Ensure checkpoint folder exists ===
os.makedirs(os.path.dirname(checkpoint_path), exist_ok=True)

print("Polars version:", pl.__version__)

# === Load paper metadata once ===
papers = pl.read_csv(papers_path).select(["AuthorId", "PaperId"])

# === Load or initialize author_paper_map ===
if os.path.exists(checkpoint_path) and os.path.getsize(checkpoint_path) > 0:
    with open(checkpoint_path, "rb") as f:
        author_paper_map = pickle.load(f)
    print("✅ Loaded checkpoint from disk.")
else:
    author_paper_map = defaultdict(list)
    print("🆕 Starting fresh — no valid checkpoint found.")


# === Read citations fully and slice manually ===
citations_df = pl.read_csv(citations_path).rename({"TargetAuthorId": "AuthorId"})

batch_size = 1_000_000
num_rows = citations_df.height

for i in range(0, num_rows, batch_size):
    batch_id = i // batch_size
    print(f"🔄 Processing batch {batch_id}")

    chunk = citations_df.slice(i, batch_size)

    # Filter only relevant authors for this chunk
    relevant_authors = chunk.select("AuthorId").unique()
    filtered_papers = papers.join(relevant_authors, on="AuthorId", how="inner")

    # Merge citations with filtered papers
    merged = chunk.join(filtered_papers, on="AuthorId", how="inner")

    # Count citations per PaperId
    paper_counts = (
        merged.group_by("PaperId")
              .len()
              .rename({"len": "CitationCount"})
    )

    relevant_papers = filtered_papers.join(paper_counts, on="PaperId", how="inner")

    # Update author-paper map
    for row in relevant_papers.iter_rows():
        aid, _, count = row
        author_paper_map[aid].append(count)

    # ✅ Save checkpoint every 5 batches
    if batch_id % 5 == 0:
        with open(checkpoint_path, "wb") as f:
            pickle.dump(author_paper_map, f)
        print(f"💾 Checkpoint saved at batch {batch_id}")

    # 🧹 Free memory
    del chunk, relevant_authors, filtered_papers, merged, paper_counts, relevant_papers
    gc.collect()

# === Compute H-index ===
def h_index(citations):
    citations.sort(reverse=True)
    return sum((i + 1) <= c for i, c in enumerate(citations))

h_index_data = [
    {"AuthorId": aid, "HIndex": h_index(cites)}
    for aid, cites in author_paper_map.items()
]
h_index_df = pl.DataFrame(h_index_data)

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

# === Merge and write output ===
final_df = h_index_df.join(gender_df, on="AuthorId", how="left")
final_df = final_df.with_columns(
    pl.when(pl.col("Gender").is_null()).then("unknown").otherwise(pl.col("Gender")).alias("Gender")
)

final_df.write_csv(output_path)
print("✅ H-index written to:", output_path)

# === Final cleanup ===
if os.path.exists(checkpoint_path):
    os.remove(checkpoint_path)
    print("🧹 Removed checkpoint after success.")

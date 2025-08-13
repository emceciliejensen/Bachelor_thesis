import polars as pl
from polars import read_csv_batched

def get_unique_authors(file, batch_size=5_000_000):
    unique_authors = set()
    reader = read_csv_batched(file, batch_size=batch_size)
    for batch in reader.next_batches(1000):
        df = batch.select(["SourceAuthorId", "TargetAuthorId"])
        unique_authors.update(df["SourceAuthorId"].to_list())
        unique_authors.update(df["TargetAuthorId"].to_list())
    return unique_authors

def sample_authors(author_ids, frac=0.1):
    import random
    sample_size = int(len(author_ids) * frac)
    return set(random.sample(list(author_ids), sample_size))

def write_filtered_edges(file, sampled_authors, output_file, batch_size=5_000_000):
    reader = read_csv_batched(file, batch_size=batch_size)
    first = True
    for batch in reader.next_batches(1000):
        df = batch.filter(
            batch["SourceAuthorId"].is_in(sampled_authors)
        )
        if df.height > 0:
            with open(output_file, "w" if first else "a") as f:
                df.write_csv(f, include_header=first)
            first = False

          

INPUT = "/home/emcj/data/MAG/Physics_Author_Citations.csv"
OUTPUT = "/home/emcj/data/MAG/Physics_Author_Citations_random_sample.csv"

authors = get_unique_authors(INPUT)
sampled = sample_authors(authors, frac=0.1)
write_filtered_edges(INPUT, sampled, OUTPUT)


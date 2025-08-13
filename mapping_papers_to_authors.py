import polars as pl

def filter_paper_author_affiliations(paper_ids_file, affiliations_file, output_file):
    paper_ids = pl.read_csv(paper_ids_file).select("PaperId").unique()

    affiliations = pl.read_csv(affiliations_file).select(["PaperId", "AuthorId"])
    filtered = affiliations.join(paper_ids, on="PaperId", how="inner")

    filtered.write_csv(output_file)

if __name__ == "__main__":
    filter_paper_author_affiliations("/home/emcj/data/MAG/Physics_FilteredPaperIds.csv", "/home/emcj/data/MAG/PaperAuthorAffiliations.csv", "/home/emcj/data/MAG/Physics_Author_Papers.csv")


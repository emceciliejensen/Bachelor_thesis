import polars as pl

def build_author_citations(paper_refs_file, paper_authors_file, output_file):
    # Load paper-author mapping
    paper_authors = pl.read_csv(paper_authors_file).select(['PaperId', 'AuthorId'])

    # Group authors by paper
    paper_to_authors = (
        paper_authors
        .group_by('PaperId')
        .agg(pl.col('AuthorId').unique())
    )

    # Turn into a dictionary for fast lookups
    paper_author_dict = {
        row['PaperId']: row['AuthorId'] for row in paper_to_authors.iter_rows(named=True)
    }

    # Load paper–paper citations
    citations = pl.read_csv(paper_refs_file, columns=['PaperId', 'PaperReferenceId'])


    with open(output_file, "w") as f:
    	f.write("SourceAuthorId,TargetAuthorId\n")
    	for row in citations.iter_rows(named=True):
        	citing = row["PaperId"]
        	cited = row["PaperReferenceId"]

        	citing_authors = paper_author_dict.get(citing, [])
        	cited_authors = paper_author_dict.get(cited, [])

        	for a1 in citing_authors:
            		for a2 in cited_authors:
                		f.write(f"{a1},{a2}\n")



if __name__ == "__main__":
    build_author_citations(
        "/home/emcj/data/MAG/PaperReferences.csv",
        "/home/emcj/data/MAG/Psychology_Author_Papers.csv",
        "/home/emcj/data/MAG/Psychology_Author_Citations.csv"
    )
#    edges = []
#    for row in citations.iter_rows(named=True):
#        citing = row['PaperId']
#        cited = row['PaperReferenceId']

#        citing_authors = paper_author_dict.get(citing, [])
#        cited_authors = paper_author_dict.get(cited, [])

#        for a1 in citing_authors:
#            for a2 in cited_authors:
#                edges.append((a1, a2))

#    pl.DataFrame(edges, schema=['SourceAuthorId', 'TargetAuthorId']).write_csv(output_file)




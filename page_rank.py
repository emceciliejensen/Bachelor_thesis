import polars as pl
import pandas as pd
import igraph as ig

df = pl.read_csv("/home/emcj/data/MAG/Medicine_Author_Citations_Genderized.csv", low_memory=True)

#Dropping duplicate edges.
edges = df.select(["SourceAuthorId", "TargetAuthorId"]).unique().to_numpy().tolist()

#Using TupleList to build graph.
g = ig.Graph.TupleList(edges, directed=True, vertex_name_attr="name")

#Computing PageRank.
pagerank_scores = g.pagerank()

#Mapping AuthorID.
author_ids = g.vs["name"]

#Creating pagerank centrality df.
pagerank_df = pd.DataFrame({
    "AuthorId": author_ids,
    "PageRank": pagerank_scores
})

#Getting gender info.
source_gender = df.select(["SourceAuthorId", "SourceGender"]).rename({
    "SourceAuthorId": "AuthorId",
    "SourceGender": "Gender"
}).unique()

target_gender = df.select(["TargetAuthorId", "TargetGender"]).rename({
    "TargetAuthorId": "AuthorId",
    "TargetGender": "Gender"
}).unique()

#Combining and deduplicate gender.
gender_df = pl.concat([source_gender, target_gender]).unique(subset=["AuthorId"])
gender_df_pd = gender_df.to_pandas()

#Merging with gender.
result = pagerank_df.merge(gender_df_pd, on="AuthorId", how="left")
result["Gender"] = result["Gender"].fillna("unknown")

#Saving to a csv.
result.to_csv("/home/emcj/data/MAG/Medicine_pagerank.csv", index=False)
print("Medicine_pagerank")

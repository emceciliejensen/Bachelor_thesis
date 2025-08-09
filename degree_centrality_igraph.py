import polars as pl
import pandas as pd
import igraph as ig

df = pl.read_csv("/home/emcj/data/MAG/Medicine_Author_Citations_Genderized.csv", low_memory=True)

#Removing duplicates and creating edges.
edges = df.select(["SourceAuthorId", "TargetAuthorId"]).unique().to_numpy().tolist()

#using TupleList to create the graph.
g = ig.Graph.TupleList(edges, directed=True, vertex_name_attr="name")

#Degree centrality.
in_deg = g.indegree()
out_deg = g.outdegree()

#Building the centrality df.
centrality_df = pd.DataFrame({
    "AuthorId": g.vs["name"],
    "InDegree": in_deg,
    "OutDegree": out_deg
})

#Gender extraction and merging.
source_gender = df.select(["SourceAuthorId", "SourceGender"]).rename({
    "SourceAuthorId": "AuthorId", "SourceGender": "Gender"
}).unique()

target_gender = df.select(["TargetAuthorId", "TargetGender"]).rename({
    "TargetAuthorId": "AuthorId", "TargetGender": "Gender"
}).unique()

gender_df = pl.concat([source_gender, target_gender]).unique(subset=["AuthorId"]).to_pandas()

#Merging gender into final df.
result = centrality_df.merge(gender_df, on="AuthorId", how="left")
result["Gender"] = result["Gender"].fillna("unknown")

#Saving to csv.
result.to_csv("/home/emcj/data/MAG/Medicine_degree_centrality_igraph.csv", index=False)
print("Medicine")

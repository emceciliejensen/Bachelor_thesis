from graph_tool.all import Graph, betweenness
import pandas as pd

df = pd.read_csv("/home/emcj/data/MAG/Sociology_Author_Citations_Genderized.csv")

#Building the graph.
g = Graph(directed=True)
id_map = {}
source_ids = df["SourceAuthorId"]
target_ids = df["TargetAuthorId"]

#Adding nodes and edges.
for src, tgt in zip(source_ids, target_ids):
    if src not in id_map:
        id_map[src] = g.add_vertex()
    if tgt not in id_map:
        id_map[tgt] = g.add_vertex()
    g.add_edge(id_map[src], id_map[tgt])

# Compute betweenness. Vertex_bet gives a betweenness score to each node
vertex_bet, _ = betweenness(g)

# Mapping it to AuthorId
inv_map = {v: k for k, v in id_map.items()}
centrality = [{"AuthorId": inv_map[v], "BetweennessCentrality": vertex_bet[v]} for v in g.vertices()]
result = pd.DataFrame(centrality)

#THen saving it as a csv.
result.to_csv("/home/emcj/data/MAG/Sociology_BetweennessCentrality.csv", index=False)
print("Betweenness centrality saved to 'Sociology_BetweennessCentrality.csv'")

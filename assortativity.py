import pandas as pd
import networkx as nx

cols = ['SourceAuthorId', 'TargetAuthorId', 'SourceGender', 'TargetGender']
df = pd.read_csv('/home/emcj/data/MAG/Medicine_Author_Citations_Genderized.csv', usecols=cols)

#Normalizing genders.
df['SourceGender'] = df['SourceGender'].replace(-2, 'unknown').fillna('unknown')
df['TargetGender'] = df['TargetGender'].replace(-2, 'unknown').fillna('unknown')

#Creating a directed graph.
G = nx.DiGraph()

#Adding nodes and edges to graph.
for row in df.itertuples(index=False):
    source = row.SourceAuthorId
    target = row.TargetAuthorId
    source_gender = row.SourceGender
    target_gender = row.TargetGender

    #Adding node if not already in graph.
    if source not in G:
        G.add_node(source, gender=source_gender)
    if target not in G:
        G.add_node(target, gender=target_gender)

    #Adding edge.
    G.add_edge(source, target)

#Computing assortativity based on gender.
assortativity = nx.attribute_assortativity_coefficient(G, 'gender')
print("Medicine")

#saving output as csv.
output_df = pd.DataFrame({
    'metric': ['gender_assortativity'],
    'value': [assortativity]
})
output_df.to_csv('/home/emcj/data/MAG/Medicine_Assortativity.csv', index=False)

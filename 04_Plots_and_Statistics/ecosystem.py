import networkx as nx
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import pprint
import operator
import itertools

from google.cloud import bigquery
from networkx.algorithms import approximation as apxa

def exec_select_query(query):
    """
    Executes the given SQL query using the static Google authentication credentials.

    :param query: The SQL query
    :return: A (pandas) dataframe that contains the results
    """
    # Initialize teh Google BigQuery client. The authentication token should be placed in the working directory in the
    # following path: /resources/google.json
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "resources", "google_bkp.json")
    #os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "..", "DatabasePusher", "resources", "google_bkp.json")
    client = bigquery.Client()

    # Execute the query and retrieve result data (as pandas dataframe)
    result_df = client.query(query).to_dataframe()

    return result_df


def analyze_ecosystem():
    """
    # TODO häufige nodes sollten größer sein +
    # TODO farben für source und target sollten unterschiedlich sein +
    # TODO farben für 1st und 3rd party (target) sollten unterschiedlich sein
    # TODO ggf beschriftung nur bei "großen nodes"
    """
    # CAST(is_first_party AS INT64) AS is_first_party , CAST(is_third_party AS INT64) AS is_third_party
    # result_df = exec_select_query("""SELECT DISTINCT etld, channelname, is_first_party, is_third_party
    #                                         FROM `hbbtv-research.hbbtv.requests`
    #                                         ORDER BY etld;""")


    result_df = exec_select_query(""" WITH channels AS (
                                          SELECT
                                            channelid
                                          FROM
                                            `hbbtv-research.hbbtv.requests`
                                          GROUP BY
                                            channelid
                                        ),

                                        hbbtv_landing AS (
                                          SELECT
                                          distinct
                                            r.channelid,
                                            r.etld,
                                            True as channel_id
                                          FROM
                                            `hbbtv-research.hbbtv.requests` r
                                          INNER JOIN
                                            channels c ON c.channelid = r.channelid
                                            where r.is_first_party
                                        ),


                                        other_etlds AS (
                                          SELECT
                                            #hl.channelid,
                                            hl.etld AS source_etld,
                                            r.etld AS target_etld
                                          FROM
                                            hbbtv_landing hl
                                          inner JOIN
                                            `hbbtv-research.hbbtv.requests` r ON hl.channelid = r.channelid
                                          WHERE
                                            r.etld != hl.etld
                                        )


                                        select distinct * from (
                                        select * from hbbtv_landing
                                        union all select *, false from other_etlds


                                        ); """)

    # Get list of src and dst values - src = channelname, dst = etld+1
    #src = set(result_df['channelid'].tolist())
    #dst = set(result_df['etld'].tolist())

    chid = list()
    for index, row in result_df.iterrows():
        channelid = row['channelid']
        is_channelid = row['channel_id']

        if is_channelid:
            chid.append(channelid)

    G = nx.from_pandas_edgelist(result_df, source='channelid', target='etld', edge_attr=['channel_id'])

    #print(f"[PY.ECO.5.5.5.1] Number of nodes: {G.number_of_nodes()}, number of edges: {G.number_of_edges()}")

    # Make different colors for src and dst nodes
    color = ['']
    for n in G.nodes():
        nx.set_node_attributes(G.nodes[n], color, "color")
        G.nodes[n]['color'] = 'b' if n in chid else 'r'

    colors = [node[1]['color'] for node in G.nodes(data=True)]


    # # Set different node-size
    d = dict(G.degree)
    #node_size = [v * 5 for v in d.values()]
    node_size = [v for v in d.values()]

    labels = dict()
    # Big nodes with label
    for node in G.nodes():
        labels[node] = len(G[node])

    labels = dict(sorted(labels.items(), key=operator.itemgetter(1), reverse=True))

    # Set options for nodes and edges
    options = {
        "node_size": node_size,
        "node_color": colors,
        "width": 1,
        "with_labels": False
    }

    # Plot graph - networkx graph
    # Alternative Visualization:
    # --- https://networkx.org/documentation/stable/auto_examples/drawing/plot_knuth_miles.html#sphx-glr-auto-examples-drawing-plot-knuth-miles-py
    # --- https://networkx.org/documentation/stable/auto_examples/drawing/plot_degree.html#sphx-glr-auto-examples-drawing-plot-degree-py
    # --- https://networkx.org/documentation/stable/auto_examples/drawing/plot_random_geometric_graph.html#sphx-glr-auto-examples-drawing-plot-random-geometric-graph-py
    #plt.figure(figsize=(16,9))
    nx.draw_networkx(G, **options)

    # Set options for nodes and edges
    # options = {
    #     "node_size": node_size,
    #     "node_color": colors,
    #     "width": 1,
    #     "with_labels": dict(itertools.islice(labels.items(), 10)) # take top 10 labels
    # }
    # nx.draw_networkx(G, **options)


    plt.tight_layout()
    #plt.show()
    plt.axis('off')
    #plt.figure(figsize=(16,9))
    #plt.savefig("Ecosystem.pdf", format="pdf", orientation='landscape')

    # Clique
    #cliques = nx.enumerate_all_cliques(G)
    #for el in list(cliques):
    #    print(el)

    max_independent_clique = apxa.maximum_independent_set(G)
    #print(f"[PY.ECO.5.5.4] Maximum independent set: {len(max_independent_clique)}")

    # Computes node connectivity
    # -> https://networkx.org/documentation/stable/reference/algorithms/approximation.html#module-networkx.algorithms.approximation.kcomponents
    h = nx.all_pairs_node_connectivity(G)
    #print(type(h), h)
    #pprint.pprint(h) # Needs to be uncommenct :)
    #print(len(h.keys()))
    total_weight = 0
    for k,v in h.items():
        key = k
        weight = 0
        for k,v in v.items():
            weight += v


        #print(k, weight)
        if weight > total_weight:
            total_weight = weight

    #print(f"[PY.ECO.5.5.3] Largest component connectivity: {total_weight}")

    # Connected components
    connected_components = nx.connected_components(G)
    #pprint.pprint(connected_components)
    # for el in list(connected_components):
    #     print(len(el))


    # Components
    # -> https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.approximation.kcomponents.k_components.html#networkx.algorithms.approximation.kcomponents.k_components
    G.remove_edges_from(nx.selfloop_edges(G)) # remove self for component
    k_components = apxa.k_components(G)
    #pprint.pprint(k_components) # Needs to be uncommenct :)

    total = list()
    for k,v in k_components.items():
        total.append(len(v[0]))
        _v = v[0] # is set
        _chid = set(chid) # convert to set
        result = _v.intersection(_chid)
        #print(f"[PY.ECO.5.5.1.1] Components contains tv channel {k, len(result)} and total {len(v[0])}")

    #print(f"[PY.ECO.5.5.1] Components: {len(k_components.keys())}")

    # avg path length
    #for C in (G.subgraph(c).copy() for c in nx.connected_components(G)):
    #    print(f"[PY.eco.5.5.2] ∅ path length: {nx.average_shortest_path_length(C)}")


    # https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.components.number_connected_components.html#networkx.algorithms.components.number_connected_components
    number_connected_components = nx.number_connected_components(G)
    #print(f"[PY.ECO.5.5.5] Number of connected components {number_connected_components}")

    sd = int(np.std(total))
    #print(f"[PY.eco.5.5.3] SD: {sd}")

    # Graph/Node degree
    graph_degree = G.degree
    node_size = dict()
    for degree in graph_degree:
        name_node = degree[0]
        edges = degree[1]

        node_size[name_node] = edges

    node_size = dict(sorted(node_size.items(), key=lambda item: item[1], reverse=True))
    sd = int(np.std(list(node_size.values())))
    #print(f"[PY.ECO.5.5.7] All nodes SD: {sd}, mean: {np.mean(list(node_size.values()))}")
    df_node_size = pd.DataFrame.from_dict(node_size, orient='index')
    #print(df_node_size)
    df_node_size.to_excel("Node_Size.xlsx")
    #print(f"[PY.ECO.5.5.6] Top 10 nodes by edges: {df_node_size.head(10)}")
    top_10 = df_node_size.head(10)
    top_10_dict = top_10.to_dict()[0]
    #print(list(top_10_dict.values()))
    sd = int(np.std(list(top_10_dict.values())))
    #print(f"[PY.ECO.5.5.6] Top 10 SD: {sd}")
    #print(f"[PY.ECO.5.5.6] Top 10 mean: {np.mean(list(top_10_dict.values()))}")
    #print(f"[PY.ECO.5.5.6.1] TVping.com edges: {node_size.get('tvping.com')}")
    #print(f"[PY.ECO.5.5.6.1]xiti.com edges: {node_size.get('xiti.com')}")
    #print(node_size)
    single_edge_nodes = 0
    total_wo_channel = 0
    for node, weight in node_size.items():
        if node not in chid:
            total_wo_channel += 1
            if weight == 1:
                single_edge_nodes += 1
    #print(f"[PY.ECO.5.5.8] Number (procentual) of single edge nodes: {single_edge_nodes} from total nodes {len(node_size.keys())} as procentual -> {single_edge_nodes/len(node_size.keys()) * 100}")
    #print(f"[PY.ECO.5.5.8.1] Number of signle edge nodes w/o channel: {single_edge_nodes} and procentual of {single_edge_nodes/total_wo_channel*100}")
    #print(f"[PY.ECO.5.5.9] Average degree connectivity {np.mean(list(nx.average_degree_connectivity(G).values()))}")

    greater_than_nodes = 0
    for node, weight in node_size.items():
        if weight >= 10:
            greater_than_nodes += 1

    #print(f"[PY.ECO.5.5.10] Nodes with atleast 10 edges: {greater_than_nodes}")



if __name__ == '__main__':
    analyze_ecosystem()

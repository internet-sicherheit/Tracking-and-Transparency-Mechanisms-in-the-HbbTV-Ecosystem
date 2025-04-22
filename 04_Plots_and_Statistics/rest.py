import ast
import matplotlib.pyplot as plt
import networkx as nx
import os
import seaborn as sns
import json
import ast
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import Levenshtein as levenshtein
import plotly.graph_objects as go

#print(go.__file__)

from fuzzywuzzy import fuzz
from google.cloud import bigquery
from datetime import datetime, timedelta
from itertools import islice
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.manifold import MDS
from scipy import stats

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


def exec_update_query(query):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "..", "DatabasePusher", "resources", "google_bkp.json")
    client = bigquery.Client()

    result = client.query(query)

    return result.result()

def cookie_analysis(profile, flag):
    """
    Computes different numbers on the cookie usage of the observed (third) parties.

    :return:  Nothing. Prints query results to standard out.
    """
    pass

    """Folgende Dinge könnten analysiert werden
    o Welcher Cookies werden von den Sendern genutzt
    o Wer setzt die cookies?
    o Wer liefert die Banner aus?
    o Wie viele Sendern nutzen Banner
    ▪ Wie viele Cookies?"""
    if flag == "syncing":
        result_df_cookies = exec_select_query(f""" SELECT DISTINCT c.name, c.origin, req.channelname
                                                    FROM
                                                      `hbbtv-research.hbbtv.requests` req,
                                                      `hbbtv-research.hbbtv.cookies` c
                                                    WHERE
                                                      req.request_id=c.request_id
                                                      AND req.scan_profile=c.scan_profile
                                                      AND NOT c.duplicate
                                                      AND req.scan_profile={profile}
                                                      ORDER BY c.name DESC """)
        # Cookie syncing
        cookies = dict()

        for index, row in result_df_cookies.iterrows():
            cookie_name = row['name']
            cookie_origin = row['origin']
            channel_name = row['channelname']

            if list(cookies.keys()).count((cookie_name, cookie_origin)) == 1:
                cookies[(cookie_name, cookie_origin)].append(channel_name)
            else:
                value = list()
                value.append(channel_name)
                cookies[(cookie_name, cookie_origin)] = value


        sankey_diagramm(cookies)

        #documents = list(cookies.values())

        #tfidf_vectorizer = TfidfVectorizer()
        #tfidf_matrix = tfidf_vectorizer.fit_transform([" ".join(doc) for doc in documents])
        #jaccard_similarities = tfidf_matrix.dot(tfidf_matrix.T)
        #n_clusters = len(list(cookies.keys()))

        #kmeans = KMeans(n_clusters=n_clusters, random_state=0)
        #kmeans.fit(jaccard_similarities)
        #cluster_assignments = kmeans.labels_

        #for i, (key, _) in enumerate(cookies.items()):
        #    print(f'Data Point {i + 1} is assigned to Cluster {cluster_assignments[i]}')

        #mds = MDS(n_components=2, dissimilarity="precomputed", random_state=1)
        #pos = mds.fit_transform(1 / (jaccard_similarities + 1e-8))

        #colors = ['r', 'g', 'b']
        #plt.figure(figsize=(8, 6))

        #for i, (key, _) in enumerate(cookies.items()):
        #    x, y = pos[i]
        #    cluster_idx = cluster_assignments[i]
        #    plt.scatter(x, y, c=colors[cluster_idx], label=f'Data Point {i + 1} (Cluster {cluster_idx})')

        #plt.legend()
        #plt.title("Cluster Visualization")
        #plt.show()

        #with open(f"{os.getcwd()}/files/Cookie_Syncing_{profile}.txt", "w") as f:
        #    f.write(f"[Q.X] Cookies per name, origin in profile {profile}\n")
        #    for k,v in cookies.items():
        #        if len(v) > 1:
        #            f.write(f"For origin {k} are {len(v)} cookies\n")

        #print(f"""[Q.X] Cookies per name, origin in profile {profile} with {len(result_df_cookies)} analyzed requests.
        #            \tOrigin: {len(result_df_cookies['origin'])}
        #            \tCookiename: {len(result_df_cookies['name'])}
        #            \tChannelname: {len(result_df_cookies['channelname'])}
        #            \tUnique Origin,Cookie tuple: {len(cookies)}""")

        #plot_distribution(cookies, profile)
        #plot_distribution_heatmap(cookies, profile)
        #for k,v in cookies.items():
        #    if len(v) > 1:
        #        print(f"For origin {k} are {len(v)} cookies")
        #        print(v)
        #        print("-------------------------")

    elif flag == "cross":
        """
        Search for Cookie values in the HTTP Traffic.
        """
        result_df_cookie_value = exec_select_query(f""" SELECT req.channelName, c.name, c.origin, c.value
                                                        FROM `hbbtv-research.hbbtv.requests` req, `hbbtv-research.hbbtv.cookies` c
                                                        WHERE req.request_id=c.request_id
                                                        AND req.scan_profile=c.scan_profile
                                                        AND NOT c.duplicate
                                                        AND req.scan_profile={profile} """)


def sankey_diagramm(data):
    """
    Ref: https://plotly.com/python/sankey-diagram/



    # todo:
    1. modify code to add node value
    2. add "others" to the diagram
    """
    # Order dict by top 10 values
    sorted_keys = sorted(data, key=lambda k: len(data[k]), reverse=True)

    sorted_dict = dict()
    sorted_keys_10 = sorted_keys[slice(10)]
    sorted_keys_15 = sorted_keys[slice(15)]
    rest_keys = list()

    for key in sorted_keys:
        if key not in sorted_keys_10:
            rest_keys.append(key)

    for key in sorted_keys_10:
        sorted_dict[key] = data[key]
    sorted_dict["Others"] = ["Others"]

    rest_dict = dict()
    for key in rest_keys:
        rest_dict[key] = data[key]

    #print(sorted_dict)



    # Create mapping to numeric
    map = dict()

    # Create keys:
    for values in list(sorted_dict.values()):
        for value in values:
            map[value] = list()



    #map["Others"] = list()

    for key, values in sorted_dict.items():
        # get the numeric number of the key ... 0-10/15
        numeric_key = list(sorted_dict.keys()).index(key) + len(list(map.keys()))
        for value in values:
            if value in list(map.keys()):
                map[value].append(numeric_key)


    #for key, values in rest_dict.items():
    #    numeric_key = list(rest_dict.keys()).index(key) + len(list(map.keys())) + 1
    #    for value in values:
    #        map["Others"].append(numeric_key)

    #print(map)

    # Build dependencies
    source = list()
    for k, v in map.items():
        for i in range(len(v)):
            source.append(list(map.keys()).index(k))

    #print(source)

    target = list()
    for values in list(map.values()):
        for value in values:
            target.append(value)

    #print(target)

    #print(len(target), len(source))

    # todo: value optimization
    value = list()
    for key in sorted_keys_10:
        _v = len(data[key])


        for i in range():
            value.append(_v)


#    for k, v in map.items():
#        channel = k
#        tracker = v
#        counter = 0
#        for _k, _v in data.items():
#            if _k == tracker and :

        #for i in range(len(v)):
        #    value.append(len(v))


    #for i in range(len(target)):
    #    value.append(1)

    #c = 0
    #for value in rest_dict.values():
#        c+=len(value)
#    value.append(c)


    # remove names for channel:
    names = list()
    for i in range(len(list(map.keys()))):
        names.append("")
    label = names + sorted_keys_10
    label.append("Others")

    #node_sizes = list()

    #for i in range(len(label)-len(sorted_keys_10)-1):
    #    node_sizes.append(1)

    #for i in range(len(sorted_keys_10)):
    #    node_sizes.append(200)

    #print(node_sizes)

    fig = go.Figure(go.Sankey(
        node = dict(
            pad = 15,
            thickness = 200,
            line = dict(color = "black", width = 0.5 ),
            label=label,
            #customdata=node_sizes
        ),
        link = dict(
            source=source,
            target=target,
            value=value
        )))

    fig.update_layout(title_text="Cookie Syncing in HbbTV<br>Representing the top 10 3rd party tracker", font_size=10)
    fig.show()


def take(n, iterable):
    """Return the first n items of the iterable as a list."""
    return list(islice(iterable, n))


def plot_distribution(distribution_function, profile):
    names, origins = zip(*distribution_function.keys())
    channels = [len(values) for values in distribution_function.values()]

    plt.figure(figsize=(10, 6))
    plt.bar(range(len(channels)), channels, tick_label=names)
    plt.xlabel("Name and Origin")
    plt.ylabel("Number of Channels")
    plt.title(f"Channel Distribution by Name and Origin for profile {profile}")
    plt.xticks(rotation=45)

    plt.show()


def plot_distribution_heatmap(data, profile):
    name_origin = list(data.keys())
    channelnames = list(data.values())

    cookie_sync = pd.DataFrame(list(zip(name_origin, channelnames)))

    cookie_sync.columns=['Name-Origin', 'Channelnames']
    #cookie_sync.index.name="Index"
    print(cookie_sync)

    plt.figure(figsize=(12, 8))
    #sns.heatmap(cookie_sync.pivot("Name-Origin", "Channelnames"), annot=True, fmt="d", cmap="YlGnBu") # does not work ...
    sns.jointplot(cookie_sync, x="Name-Origin", y="Channelnames")
    plt.title(f"Channel Distribution by Name and Origin Clusters for profile {profile}")
    plt.xticks(rotation=45)

    plt.show()


def data_leakage():
    """
    Computes how often different personal information (e.g., watched TV show or information on the used device) are
    shared with partners.

    :return: Nothing. Prints query results to standard out.
    """

    # To how many (and which) parties was the watched channel leaked? [Q3.1]
    result_df = exec_select_query("""SELECT etld, channelname, queryString
                                        FROM `hbbtv-research.hbbtv.requests`
                                        WHERE lower(queryString) LIKE lower(CONCAT('%', SPLIT(REPLACE(channelname, "_", ""), ' ')[OFFSET(0)], '%'));""")
    print("[Q3.1] watched channel leaked to party:\n ", result_df)

    # To how many (and which) parties was the used TV leaked? [Q3.2]
    # We check for the model name (43UK6300LLB), TV manufacturer (lge), the used OS version (WEBOS4.0 05.40.26),
    # and a hardware specific identifier (W4_LM18A).
    result_df = exec_select_query("""SELECT DISTINCT(etld), channelname, queryString
                                        FROM `hbbtv-research.hbbtv.requests`
                                        WHERE lower(queryString) LIKE '%lge%'
                                            OR lower(queryString) LIKE '%43UK6300LLB%'
                                            or lower(queryString) LIKE '%WEBOS4.0 05.40.26%'
                                            or lower(queryString) LIKE '%W4_LM18A%'; """)
    print("[Q3.2] Leaked TV:\n ", result_df)

    # # Is the watched program leaked? [Q3.3]
    # r1 = get_leaked_program()
    # print("[Q3.3]", r1)


def get_leaked_program():
    # TODO Nurullah wollte hier was machen, umd die anfragen für BQ zu optimieren

    # First, we want to get the program that was shown.
    show_names = set()
    result_df = exec_select_query("""SELECT current_channel, result
                                        FROM `hbbtv-research.hbbtv.LogData`
                                        WHERE command = "get_current_channel_program_info"; """)
    result_df = result_df.reset_index()  # make sure indexes pair with number of rows
    for index, row in result_df.iterrows():
        channel_info = ast.literal_eval(row['result'])
        show_names.add(channel_info.get('programName', None))

    show_names.remove(None)
    print(show_names)

    # Second, we need to check if any of these shows were leaked via HTTP traffic.

    for show_name in show_names:
        qs = "SELECT * FROM `hbbtv-research.hbbtv.requests` WHERE lower(queryString) LIKE '%" + str(show_name).lower() + "%';"
        result_df = exec_select_query(qs)
        print(qs, result_df)

    return []


def general_statistics():
    """
    Computes some general (simple) statistics on the measurement.

    :return: Nothing. Prints query results to standard out.
    """

    # How many channels did we analyze? [Q1.1]
    result_df = exec_select_query("""SELECT COUNT(DISTINCT channelname)
                                        FROM `hbbtv-research.hbbtv.requests`;""")
    print("[Q1.1] number of analyzed channels: ", result_df)

    # How many known trackers (according to EasyList) did we observe? [Q1.2]
    result_df = exec_select_query("""SELECT COUNT (DISTINCT url)
                                        FROM `hbbtv-research.hbbtv.requests`
                                        WHERE is_known_tracker;""")
    print("[Q1.2] number of known trackers: ", result_df)

    # How often is a (third) party used by different TV channels? [Q1.3]
    result_df = exec_select_query("""SELECT etld, COUNT(DISTINCT channelname) as number
                                        FROM `hbbtv-research.hbbtv.requests`
                                        GROUP BY etld
                                        ORDER BY number DESC;""")
    print("[Q1.3] How often is an eTLD used by different TV stations?", result_df)


def analyze_ecosystem():
    result_df = exec_select_query("""SELECT DISTINCT etld, channelname
                                            FROM `hbbtv-research.hbbtv.requests`
                                            ORDER BY etld;""")

    G = nx.from_pandas_edgelist(result_df, source='channelname', target='etld')

    # TODO häufige nodes sollten größer sein
    # TODO farben für source und target sollten unterschiedlich sein
    # TODO farben für 1st und 3rd party (target) sollten unterschiedlich sein
    # TODO ggf beschriftung nur bei "großen nodes"
    nx.draw_networkx(G, node_size=15, with_labels=False)
    plt.tight_layout()
    plt.show()

    # pos = nx.spring_layout(G, k=0.5, seed=9843658)
    # nodes
    # nx.draw_networkx_nodes(G, pos)
    #
    # # edges
    # nx.draw_networkx_edges(G, pos)  # using a 10x scale factor here
    #
    # # labels
    # nx.draw_networkx_labels(G, pos, font_size=10, font_family="sans-serif")
    # ax = plt.gca()
    # ax.margins(0.08)
    # plt.axis("off")

    # --------- old stuff but maybe needfull ----------------
    # TODO häufige nodes sollten größer sein
    # nodes = G.nodes
    # edges = G.edges

    #print(edges)

    #for e in edges:

    #    print(e, G[e[0]], "\n")

    #for n in G.nodes(data=True):
    #    print(n)

    #for n in nodes:
    #    print(type(n), n)
    # labels = []
    # nx.set_node_attributes(G, labels, "labels")
    # labels.append("foo")
    # #print(G['MTV'])
    #
    # # TODO farben für source und target sollten unterschiedlich sein
    # options = {
    #     "node_size":15,
    #     #"node_color": "#034efc",
    #     #"edge_color": "#fc0303",
    #     "width": 1,
    #     "edge_cmap": plt.cm.Blues,
    #     "with_labels": False
    # }
    # # TODO farben für 1st und 3rd party (target) sollten unterschiedlich sein
    #
    # #attr = list(zip(result_df.is_first_party, result_df.is_first_party))
    # #print(attr)
    #
    # party = ""
    # if party == "is_first_party":
    #     attr = {'party': 'first', 'color': '#034efc'}
    # elif party == "is_third_party":
    #     attr = {'party': 'third', 'color': '#034ecf'}

    #attr = {'party': 'third', 'color': '#abcabc'}
    #nx.set_node_attributes(G, attr, name="color")
    # TODO ggf beschriftung nur bei "großen nodes"


    #print(nx.get_node_attributes(G, 'party').values())

    #print(nodes)

    #print("\n")
    #print(G.edges)



    # pos = nx.spring_layout(G, k=0.5, seed=9843658)
    # nodes
    # nx.draw_networkx_nodes(G, pos)
    #
    # # edges
    # nx.draw_networkx_edges(G, pos)  # using a 10x scale factor here
    #
    # # labels
    # nx.draw_networkx_labels(G, pos, font_size=10, font_family="sans-serif")
    # ax = plt.gca()
    # ax.margins(0.08)
    # plt.axis("off")
    # r = np.random.RandomState(seed=5)
    # ints = r.random_integers(1, 10, size=(3,2))
    # a = ['A', 'B', 'C']
    # b = ['D', 'A', 'E']
    # df = pd.DataFrame(ints, columns=["weight", "cost"])
    # df[0] = a
    # df['b'] = b
    # df[["weight", "cost", 0, "b"]]
    #
    # print(df)
    #
    # # Init graph
    # g = nx.from_pandas_edgelist(df, 0, 'b', ["weight", "cost"])
    #
    # print(g)


def traffic_cover():
    """
    Percentage of TV traffic we could analysze
    """
    # get the number of https reqeusts
    resutl_df_https = exec_select_query("""SELECT DISTINCT COUNT(URL) FROM `hbbtv-research.hbbtv.requests` WHERE url LIKE 'https%';""")
    resutl_df_total = exec_select_query("""SELECT DISTINCT COUNT(URL) FROM `hbbtv-research.hbbtv.requests`;""")

    # TODO get Percentage
    # return results_df_https[value]/result_df_total[value]*100


def cookieSecurity():
    """
    Statistic of cookie security - httpOnly, secure and sameSite - Flag
    """
    result_df_http_only = exec_select_query("""SELECT DISTINCT c.name, c.origin, req.channelname, c.http_only
                                                FROM `hbbtv-research.hbbtv.cookies` c, `hbbtv-research.hbbtv.requests` req
                                                WHERE c.request_id = req.request_id
                                                ORDER BY c.name asc ;""")


    #result_df_secure = exec_select_query("""SELECT count(*) FROM `hbbtv-research.hbbtv.cookies` WHERE secure = 1;""")
    #result_df_sameSite = exec_select_query("""SELECT count(*) FROM `hbbtv-research.hbbtv.cookies` WHERE sameSite1 = 1;""")


def request_response(profile):
    result_df_request = exec_select_query(f""" SELECT count(*) FROM `hbbtv-research.hbbtv.requests` WHERE scan_profile={profile}; """)
    result_df_response = exec_select_query(f""" SELECT count(*) FROM `hbbtv-research.hbbtv.responses` WHERE scan_profile={profile}; """)

    print(f"For profile {profile}:")
    print(f"Requests: {result_df_request}")
    print(f"Responses: {result_df_response}")


def first_third_party(profile):
    result_df_first_party = exec_select_query(f""" SELECT count(*) FROM  `hbbtv-research.hbbtv.requests`
                                                    WHERE is_first_party=true
                                                    AND scan_profile={profile}; """)
    result_df_third_party = exec_select_query(f""" SELECT count(*) FROM  `hbbtv-research.hbbtv.requests`
                                                    WHERE is_third_party=true
                                                    AND scan_profile={profile}; """)

    print(f"[Q 5.0] number of first party {result_df_first_party['f0_'][0]} and third party {result_df_third_party['f0_'][0]} for profile {profile} ")


def identify_iptv():

    """


    """
    result_df = exec_select_query(f""" SELECT DISTINCT req.channelname
                                        FROM `hbbtv-research.hbbtv.requests` req
                                        WHERE (req.url LIKE '%.mp4%' OR req.url LIKE '%.m3u%')
                                        AND req.scan_profile=1 """)


def known_tracker():
    result_df_is_known_tracker = exec_select_query(f""" SELECT count(*) FROM `hbbtv-research.hbbtv.requests`
                                                        WHERE is_known_tracker=true;""")

    result_df_third = exec_select_query(f""" SELECT count(*) FROM `hbbtv-research.hbbtv.requests`
                                                        WHERE is_known_tracker=true AND is_third_party=true;""")

    result_df_first = exec_select_query(f""" SELECT count(*) FROM `hbbtv-research.hbbtv.requests`
                                                        WHERE is_known_tracker=true AND is_first_party=true;""")


    print(f"[Q 5.3.1] Known tracker from EasyList: {result_df_is_known_tracker['f0_']}")
    print(f"[Q 5.3.2] Known First Party tracker from EasyList: {result_df_first['f0_']}")
    print(f"[Q 5.3.3] Known Third Party tracker from Easylist: {result_df_third['f0_']}")


def number_of_hbbtv_channel():
    result_df_hbbtv = exec_select_query(f""" SELECT count(distinct channelId)
                                            FROM `hbbtv-research.hbbtv.channel_details` ch
                                            WHERE ch.channelid
                                            IN (SELECT req.channelid FROM `hbbtv-research.hbbtv.requests` req); """)

    result_df_hbbtv_sat = exec_select_query(f""" SELECT satelliteName, count(distinct channelName)
                                                FROM `hbbtv-research.hbbtv.channel_details` ch
                                                WHERE ch.channelid
                                                IN (SELECT req.channelid FROM `hbbtv-research.hbbtv.requests` req)
                                                GROUP BY satelliteName; """)


    print(f"[Q 5.1.1] Number of analyzed HbbTV channel: {result_df_hbbtv['f0_']}")
    sat_name = result_df_hbbtv_sat['satelliteName'].tolist()
    number_of_channel = result_df_hbbtv_sat['f0_'].tolist()
    for i in range(len(sat_name)):
        print(f"[Q 5.1{1+i+1}] Number of analyzed HbbTV channel from satellite {sat_name[i]}: {number_of_channel[i]}")


def cookie_statistics():
    #result_df_number_cookie_jar = exec_select_query(f""" SELECT count(c.name) FROM `hbbtv-research.hbbtv.tv_cookie_store` c
    #                                                    WHERE c.name
    #                                                    NOT IN (SELECT cookie_name FROM `hbbtv-research.hbbtv.requests` r); """)

    result_df_cookies = exec_select_query(f""" SELECT cookies
                                                FROM `hbbtv-research.hbbtv.requests`
                                                WHERE cookies!="[]" """)

    cookies_http = list()
    for index, row in result_df_cookies.iterrows():
        raw_cookies = row['cookies']
        cookies = ast.literal_eval(raw_cookies)
        # data = json.loads(raw_cookie) # Does not work due to *wrong* quotes!
        for cookie in cookies:
            cookies_http.append(cookie['name'])

    result_df_cookie_jar = exec_select_query(f""" SELECT name FROM `hbbtv-research.hbbtv.tv_cookie_store` """)
    cookie_jar = result_df_cookie_jar['name'].tolist()

    cookies_http_set = set(cookies_http)
    cookie_jar_set = set(cookie_jar)

    diff = cookie_jar_set.difference(cookies_http_set)
    print(len(diff))


def channelStatistics(profile):
    result_df_channel = exec_select_query(f"""  SELECT channelid
                                              FROM `hbbtv-research.hbbtv.requests`
                                              WHERE scan_profile={profile} """)
    channel_ids_db = result_df_channel['channelid']

    with open(os.getcwd()+'/../../Measuremement_Framework/remote_control/channellist_2023_08_24_ground_truth.txt', 'r') as f:
        channellist = json.loads(f.readline())

    channel_ids_list = list()
    for channel in channellist:
        channelId = channel['channelId']
        channel_ids_list.append(channelId)

    channel_ids_db_set = set(channel_ids_db)
    channel_ids_list_set = set(channel_ids_list)

    diff = channel_ids_list_set.difference(channel_ids_db_set)
    print(f"Number of channels missing thorught the original channellist for profile {profile}: {len(diff)}, Channels:")
    for ch in list(diff):
        print(ch)


def get_id_from_cookies(profile):
    """
    Find IDs in cookie values:
    1. Eliminate all ID candidates that were observed for multiple profiles
    2. Eliminate ID candidates with the same key but different value lenght
    3. Eliminate candidates whose values do not contain enought entropy (Levenshtein ratio)
    4. Exclude candidates whose length is too short to contain enough entropy to hold an ID (at least 8 characters)

    string similarity:
        - Levenshtein
        -> levenshtein.ratio(str1, str2)
        -> levenshtein.distance(str1, str2)

    """
    #result_df = exec_select_query(f""" SELECT c.value, c.name, c.origin, req.channelname
    #                                    FROM `hbbtv-research.hbbtv.cookies` c, `hbbtv-research.hbbtv.requests` req
    #                                    WHERE c.request_id = req.request_id
    #                                    AND req.scan_profile={profile}
    #                                    AND req.is_known_tracker""")


    values = list()
    uuids = list()
    for index, row in result_df.iterrows():
        name = row['name']
        raw_value = row['value']

        if "uuid" in name:
            uuids.append(raw_value)

        if ":" in value:
            value_list = raw_value.split(":")
        elif "&" in value:
            value_list = raw_value.split("&")
        else:
            value = ast.literal_eval(raw_value)


    # 1.
    # 2.
    # 3.
    # 4.
    #if len(value) > 7:
        # # TODO:


if __name__ == '__main__':
    # measurement_overview()
    # correlation_profile_http_cookies()
    # # Get some general numbers on the measurement.
    # general_statistics()
    #
    # Analyze observed trackers (e.g., tracking pixel or TV fingerprinting).
    # get_pixel_tracker()
    # get_fingerprinters()
    #
    # # Analyze personal data leaked via HTTP traffic.
    # data_leakage()

    # TODO
    # get_leaked_program()

    # analyze_ecosystem()

    # request_response(6)

    # first_third_party(1)

    # identify_iptv()

    # cookie_statistics()

    # channelStatistics(1)
    # channelStatistics(3)
    # channelStatistics(4)
    # channelStatistics(5)
    # channelStatistics(6)

    # missingScreenshots(1)
    # missingScreenshots(3)
    # missingScreenshots(4)
    # missingScreenshots(5)
    # missingScreenshots(6)

    # cookie_statistics()

    cookie_analysis(5, "syncing")
    # get_id_from_cookies(3)
    # for i in [1, 3,4,5,6]:
    #    cookie_analysis(i, "syncing")
    # cookie_analysis(i, "cross")

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
import tldextract

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

def measurement_overview():
    """
    Delivers numbers for the measurement overview in 4.7

    :Return None: prints out
    """

    date_df = pd.DataFrame({'scan_profile': [1,3,4,5,6]})

    # Number of analyzed channels
    result_df_channels = exec_select_query("""SELECT scan_profile, COUNT (DISTINCT req.channelid) AS number_channels
                                                FROM `hbbtv-research.hbbtv.requests` AS req
                                                GROUP BY scan_profile ;""")

    merged_df = pd.merge(date_df, result_df_channels, on='scan_profile')
    #print(merged_df.to_latex(index=False))
    #print(f"[Q4.7.1] Max channels: {merged_df['number_channels'].max()}, Min channels {merged_df['number_channels'].min()}")


    # Channels per satellite
    result_df_satellite = exec_select_query(""" SELECT foo.satelliteName, count(*) FROM (
                                                  SELECT
                                                    DISTINCT channelname,
                                                    satelliteName
                                                  FROM
                                                    `hbbtv.channel_details`) AS foo GROUP BY foo.satelliteName ORDER BY foo.satelliteName """)

    #print(f"[Q4.7.2] {result_df_satellite['f0_'].tolist()[0]} Astra 1L, {result_df_satellite['f0_'].tolist()[2]} Hot Bird 13E, {result_df_satellite['f0_'].tolist()[1]} Eutelsat")

    # HTTP requests/respones
    result_df_http_requests_responses = exec_select_query(""" SELECT count(*) FROM `hbbtv.requests`""")
    #print(f"[Q4.7.3] Caputred {result_df_http_requests_responses['f0_'].tolist()[0]} HTTP requests/responses")

    # Cookie Jar
    result_df_cookies = exec_select_query(""" SELECT storage_type, count(*) as export_data
                                                FROM `hbbtv.tv_cookie_store`
                                                GROUP BY storage_type
                                                ORDER BY storage_type; """)
    #print(f"[Q4.7.4] Cookie Jar: {result_df_cookies['export_data'].tolist()[0]}, Local Storage: {result_df_cookies['export_data'].tolist()[1]}")

    # Kruskal-Wallis on http requests button
    result_df_http_request_by_profile = exec_select_query(""" SELECT
                                                              scan_profile,
                                                              channelid,
                                                              COUNT(*) AS traffic
                                                            FROM
                                                              `hbbtv-research.hbbtv.requests`
                                                            GROUP BY
                                                              channelid,
                                                              scan_profile
                                                            ORDER BY
                                                              scan_profile; """)

    grouped_df_traffic = result_df_http_request_by_profile.groupby('scan_profile')

    df1 = grouped_df_traffic.get_group(1)['traffic']  # normal
    df3 = grouped_df_traffic.get_group(3)['traffic']  # red
    df4 = grouped_df_traffic.get_group(4)['traffic']  # yellow
    df5 = grouped_df_traffic.get_group(5)['traffic']  # blue # TODO
    df6 = grouped_df_traffic.get_group(6)['traffic']  # green

    kruskal_profile_http = stats.kruskal(df1, df3, df4, df5, df6)
    n = len(result_df_http_request_by_profile)
    print(f"""[Q4.7.5] button click on http requests  p-value < {kruskal_profile_http[1]},
                eta square {compute_eta_square(kruskal_profile_http[0], 5,n)}""")

    # Kruskal-Wallis on Cookie Jar and Local Storage

    result_df_all_cookies = exec_select_query(""" SELECT
                                                  origin,
                                                  name,
                                                  path,
                                                  COUNT(*) AS export_data
                                                FROM
                                                  `hbbtv.cookies`
                                                GROUP BY
                                                  origin,
                                                  name,
                                                  path
                                                ORDER BY
                                                  origin; """)

    result_df_all_cookies.insert(0, "storage_type", "http")


    result_df_tv_export = exec_select_query(""" SELECT
                                                storage_type,
                                                  name,
                                                  path,
                                                  COUNT(*) AS export_data
                                                FROM
                                                  `hbbtv-research.hbbtv.tv_cookie_store`
                                                GROUP BY
                                                  name,
                                                  path,
                                                  storage_type """)

    result_df_tv_export.insert(1, "origin", "")

    frames = [result_df_all_cookies, result_df_tv_export]
    result_df = pd.concat(frames)

    grouped_df_export = result_df.groupby('storage_type')

    df1 = grouped_df_export.get_group('cookie jar')['export_data'] # cookie jar
    df2 = grouped_df_export.get_group('local storage')['export_data'] # local storage
    df3 = grouped_df_export.get_group('http')['export_data'] # cookies

    kruskal_profile_export_cookies = stats.kruskal(df1, df3)
    kruskal_profile_export_local_storage = stats.kruskal(df2, df3)
    n = len(result_df_cookies)

    print(f"""[Q4.7.7] cookie jar and cookies:  p-value < {kruskal_profile_export_cookies[1]}
                ,eta square {compute_eta_square(kruskal_profile_export_cookies[0], 2, n-1)}""")

    print(f"""[Q4.7.8] local storage and cookies: p-value < {kruskal_profile_export_local_storage[1]}
                ,eta square {compute_eta_square(kruskal_profile_export_local_storage[0], 2, n-1)}""")

    # Collected GB of data
    result_df_db_size = exec_select_query(""" SELECT SUM(total_physical_bytes)/pow(10,9) AS total_physical_bytes
                                                FROM `hbbtv-research`.`region-us`.INFORMATION_SCHEMA.TABLE_STORAGE
                                                WHERE table_schema="hbbtv"; """)
    #print(f"[Q4.7.8] We collected {int(result_df_db_size['total_physical_bytes'].tolist()[0])} GB of data")

    # Watchtime
    watchtime = 0
    for index, channel_info in result_df_channels.iterrows():
        scan_profile = channel_info['scan_profile']
        number_of_channels = channel_info['number_channels']


        watchtime += number_of_channels * 910


    print(f"[Q4.7.9] We watched {int(watchtime/60/60)} hours TV")

def compute_eta_square(h, k, n):
    """
    Computes the eta^2 ("eta square") value:
    eta2[h] = (h - k + 1) / (n - k)

    :param h: The result of the test. Not the p-value!
    :param k: The number of different categories
    :param n: The numbe rof samples
    :return:  The computed eta^2 value
    """
    return (h - k + 1) / (n - k)

if __name__ == '__main__':
    measurement_overview()

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

def fixTraffic():
    """
    Fix rows in bigquery database with wrong channelnumber and channelid.

    : Returns nothing - changes in BQ database
    """
    channel_groups = {"ard":[], "prosiebensat1":[]}
    result_df_referer_headers = exec_select_query(f""" SELECT
                                                          req.channelname,
                                                          req.channelid,
                                                          req.request_id,
                                                          req.headers
                                                        FROM
                                                          `hbbtv-research.hbbtv.requests` req
                                                        WHERE
                                                          req.headers LIKE '%Referer%'
                                                        ORDER BY
                                                          req.request_id
                                                         LIMIT 100 """)

    # Get the referer
    for header in ast.literal_eval(result_df_referer_headers['headers'][0]):
        if header['name'] == "Referer":
            first_referer = header['value']

    #print(first_referer, type(first_referer))

    #first_referer = list()
    ch_referer = dict()

    for index, row in result_df_referer_headers.iterrows():
        channelname = row['channelname']
        channelid = row['channelid']
        request_id = row['request_id']
        #print(row['headers'].replace("'", "\""))
        #headers = json.loads(row['headers'].replace("'", "\""))
        headers = ast.literal_eval(row['headers'])

        for header in headers:
            if header['name'] == "Referer":
                referer = header['value']
                break

        if list(ch_referer.keys()).count(channelname) == 1:
            ch_referer[channelname].append(referer)
        else:
            l = list()
            l.append(referer)
            ch_referer[channelname] = l


    for k,v in ch_referer.items():
        print(k, v, len(v))



if __name__ == '__main__':
    fixTraffic()

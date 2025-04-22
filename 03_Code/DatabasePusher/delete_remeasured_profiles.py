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
import glob

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
    #os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "resources", "google_bkp.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "..", "DatabasePusher", "resources", "google_bkp.json")
    client = bigquery.Client()

    # Execute the query and retrieve result data (as pandas dataframe)
    result_df = client.query(query).to_dataframe()

    return result_df


def exec_update_query(query):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "..", "DatabasePusher", "resources", "google_bkp.json")
    client = bigquery.Client()

    result = client.query(query)

    return result.result()

def get_remeasured_channel(path):
    """
    Get all measured channel from a result.

    return: dict
    """
    search = "channellist*"
    #os.chdir(path)
    files = glob.glob(path+search)

    ch_list = list()

    for file in files:
        with open(file, "r") as f:
            json_obj = json.loads(f.readline())
            ch_list = ch_list + json_obj

    return ch_list

def delete_remeasrued_entries(data, profile):
    """
    Delete all entries in requests, responses and cookies from the given profile.

    return: none, print out result
    """
    for channel in data:
        chid = channel['channelId']

        query_delete_requests = """ DELETE FROM `hbbtv-research.hbbtv.requests`
                                    WHERE scan_profile=%s
                                    AND channelid=\"%s\" """ % (profile, chid)

        query_delete_responses = """ DELETE FROM `hbbtv-research.hbbtv.responses`
                                    WHERE scan_profile=%s
                                    AND channel_id=\"%s\" """ % (profile, chid)


        query_delete_cookies = """ DELETE FROM `hbbtv-research.hbbtv.cookies`
                                    WHERE scan_profile=%s
                                    AND request_id IN (
                                    SELECT request_id FROM `hbbtv-research.hbbtv.responses`
                                                                WHERE scan_profile=%s
                                                                AND channel_id=\"%s\") """ % (profile, profile, chid)


        #print(query_delete_requests)
        #print(query_delete_responses)
        #print(query_delete_cookies)
        print(exec_update_query(query_delete_requests))

        print(exec_update_query(query_delete_cookies))
        print(exec_update_query(query_delete_responses))




if __name__ == '__main__':
    l = get_remeasured_channel("C:/Users/boett/Documents/GitHub/hbbtv-2022/03_results/Measurements/117_Measurement - Profil 3 Nachmessung merged/")
    delete_remeasrued_entries(l, 3)

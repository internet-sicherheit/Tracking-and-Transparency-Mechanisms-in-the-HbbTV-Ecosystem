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


def table_1():
    """
    Returns the input data for table 1:  Overview of the high-level results of our experiment per profile
    """
    for i in [1,3,4,5,6]:
        profile = getProfileName(i)
        channel_number = getChannelNumber(i)
        total_traffic, https = traffic_cover(i)
        total_traffic = total_traffic-https
        cookie_number = exec_select_query(f""" SELECT count(cookies)
                                                FROM `hbbtv-research.hbbtv.requests`
                                                WHERE cookies!="[]" AND scan_profile={i}  """)['f0_'][0]
        first_party, third_party = first_third_party(i)
        jar, storage = getCookiesFromJarStorage(i)

def getCookiesFromJarStorage(profile):
    result_df_jar =
    result_df_storage =

    return result_df_jar['f0_'][0], result_df_storage['f0_'][0]

def first_third_party(profile):
    result_df_first_party = exec_select_query(f""" SELECT count(*) FROM  `hbbtv-research.hbbtv.requests`
                                                    WHERE is_first_party=true
                                                    AND scan_profile={profile}; """)
    result_df_third_party = exec_select_query(f""" SELECT count(*) FROM  `hbbtv-research.hbbtv.requests`
                                                    WHERE is_third_party=true
                                                    AND scan_profile={profile}; """)

    return result_df_first_party['f0_'][0], result_df_third_party['f0_'][0]


def traffic_cover(profile):
    """
    Percentage of TV traffic we could analysze
    """
    # get the number of https reqeusts
    resutl_df_https = exec_select_query(f"""SELECT DISTINCT COUNT(URL) FROM `hbbtv-research.hbbtv.requests` WHERE url LIKE 'https%' AND scan_profile={profile};""")
    resutl_df_total = exec_select_query(f"""SELECT DISTINCT COUNT(URL) FROM `hbbtv-research.hbbtv.requests` WHERE scan_profile={profile};""")

    return resutl_df_total['f0_'], resutl_df_https['f0_']

def getChannelNumber(profile):
    """
    returns the amount of channel in a measurement profile.
    """
    result_df = exec_select_query(f""" SELECT count(DISTINCT channelid)
                                    FROM `hbbtv-research.hbbtv.requests`
                                    WHERE scan_profile={profile} """)

    return result_df['f0_']

def getProfileName(number=1):
    """
    Converts the channel number into the textual Visualization
    """
    profile_button_name = ""
    if number == 1:
        profile_button_name = "General"
    elif number == 3:
        profile_button_name = "Red"
    elif number == 4:
        profile_button_name = "Yellow"
    elif number == 5:
        profile_button_name = "Blue"
    elif number == 6:
        profile_button_name = "Green"

    return profile_button_name

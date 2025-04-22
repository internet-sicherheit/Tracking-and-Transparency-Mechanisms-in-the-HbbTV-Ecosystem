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
    #os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "resources", "google_bkp.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "..", "DatabasePusher", "resources", "google_bkp.json")
    client = bigquery.Client()

    # Execute the query and retrieve result data (as pandas dataframe)
    result_df = client.query(query).to_dataframe()

    return result_df

def exec_update_query(query):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "..", "DatabasePusher", "resources",
                                                                "google_bkp.json")
    client = bigquery.Client()
    result = client.query(query)
    return result.result()

def analyze_cookies(scan_profile):
    """
    :param scan_profile:
    :return:
    """
    # Get all cookies
    all_cookies = exec_select_query(f""" SELECT * FROM
                                        `hbbtv.cookies`
                                        WHERE scan_profile={scan_profile}""")

    for index, cookie in all_cookies.iterrows():
        req_id = cookie['request_id']
        name = cookie['name']
        origin = cookie['origin']
        value = cookie['value']


        print(req_id)

        get_issuing_cookie(req_id, scan_profile, name, origin, value)


def get_issuing_cookie(request_id, scan_profile, s_name, s_origin, s_value):
    """
    Get the data for the issuing cookie.

    :param request_id:
    :param scan_profile:
    :param s_name: Selected cookie name
    :param s_origin: Selected cookie origin
    :param s_value: Selected cookie value
    :return:
    """
    # Get the http request to the cookie
    query = f""" SELECT url, cookies
                FROM `hbbtv-research.hbbtv.responses`
                 WHERE request_id={request_id}
                 AND scan_profile={scan_profile}
                 AND cookies!="[]" """

    cookie_setting_request = exec_select_query(query)

    print(f"Found {len(cookie_setting_request)} rows for request_id {request_id} and profile {scan_profile}")


    for index, cookie_request in cookie_setting_request.iterrows():

        result_row = cookie_setting_request.iloc[:1]
        raw_cookies = cookie_request['cookies']
        cookies = ast.literal_eval(raw_cookies)

        for cookie in cookies:
            # Get values for each cookie
            name = cookie['name']
            origin = tldextract.extract(cookie_request['url']).subdomain + '.' + tldextract.extract(
                    cookie_request['url']).domain + '.' + tldextract.extract(cookie_request['url']).suffix
            value = cookie['value']
            http_only = cookie['httpOnly']
            secure = cookie['secure']
            sameSite = cookie['sameSite']

            # Check if the cookie is the searched one !
            # One request can have multiple cookies - each cookie is stored seperatly in the db
            if s_name == name and s_origin == origin and s_value == value:
            #    print("Update Cookie securtiy")
                update_cookie(request_id, scan_profile, s_name, s_origin, s_value,http_only, secure, sameSite)
            #else:
            #    print("syntax scan failed:", s_name, name, s_origin, origin, s_value, value)


def update_cookie(request_id, scan_profile, s_name, s_origin, s_value,http_only, secure, sameSite, update=False):
    """
    Update the cookie row in the db.

    :param request_id:
    :param scan_profile:
    :param s_name: Selected cookie name
    :param s_origin: Selected cookie origin
    :param s_value: Selected cookie value
    :param http_only: New value for http_only
    :param secure: New value for secure
    :param sameSite: New value for sameSite
    :return:
    """
    if update:
        pass
    else:
        update_count = exec_select_query(f""" SELECT COUNT(*) AS count
                                                FROM `hbbtv.cookies`
                                                WHERE request_id={request_id}
                                                AND scan_profile={scan_profile}
                                                AND name=\"{s_name}\"
                                                AND origin=\"{s_origin}\"
                                                AND value=\"{s_value}\";""")
        affected_rows = update_count.iloc[:1]['count'][0]
        if affected_rows == 1:
            print("I would have updated %d rows! Aborting. Debug: request=%d profile=%d" % (
                affected_rows, request_id, scan_profile))
            return
        else:
            print("Updating! Debug: request=%d profile=%d affected rows:%d"  % (request_id, scan_profile, affected_rows))


if __name__ == '__main__':
    analyze_cookies(6)

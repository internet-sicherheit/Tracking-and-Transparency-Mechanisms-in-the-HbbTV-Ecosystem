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
import re
import time

from fuzzywuzzy import fuzz
from google.cloud import bigquery
from datetime import datetime, timedelta
from itertools import islice
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.manifold import MDS
from scipy import stats
from datetime import datetime
from difflib import SequenceMatcher as SM

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
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "..", "DatabasePusher", "resources",
                                                                "google_bkp.json")
    client = bigquery.Client()
    result = client.query(query)
    return result.result()

def insertInto(query):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "..", "DatabasePusher", "resources",
                                                                "google_bkp.json")
    client = bigquery.Client()
    result = client.query(query)
    return result.result()

def test_analyze():
    result_df_distinct_values = exec_select_query(""" SELECT distinct c.value
                                                        FROM `hbbtv.cookies` c
                                                        WHERE BYTE_LENGTH(c.value) > 10
                                                        AND BYTE_LENGTH(c.value) < 180; """)

    values = result_df_distinct_values['value'].tolist()

    possible_syncing = dict()

    for value in values:
        value = value.replace('"', '\\"')
        value = value.replace("'", "\\'")
        query = f""" SELECT * FROM `hbbtv-research.hbbtv.requests` req WHERE req.url LIKE "%{value}%"; """
        print(value)
        result_df_syncing = exec_select_query(query)

        if len(result_df_syncing) > 0:
            if list(possible_syncing.keys()).count(value) == 0:
                for index, row in result_df_syncing.iterrows():
                    request_id = row['request_id']
                    l = [request_id]
                    possible_syncing[value] = l
            else:
                for index, row in result_df_syncing.iterrows():
                    request_id = row['request_id']
                    possible_syncing[value].append(request_id)

        with open('possible_syncing.txt', 'w') as f:
            f.write(json.dumps(possible_syncing))


def analyze_http_requests():
    """
    Analyze the http response for the cookie value.

    508 URLs which are like cookie values in other requests than the first

    """
    #all_responses = exec_select_query(f""" SELECT url FROM `hbbtv.requests` """)

    all_distinct_cookie_values = exec_select_query(f""" SELECT distinct c. origin, c.name, c.value, c.scan_profile, c.request_id
                                                        FROM `hbbtv.cookies` c
                                                        WHERE BYTE_LENGTH(c.value) > 10
                                                        AND BYTE_LENGTH(c.value) < 25
                                                        AND NOT duplicate; """)

    c_no = 0
    c_possible = 0

    for index,cookie in all_distinct_cookie_values.iterrows():
        #print(type(cookie))
        origin = cookie['origin']
        name = cookie['name']
        value = cookie['value']
        src_req_id = cookie['request_id']

        syncing_candidates = ""
        try:
            value = value.replace('"', '\\"')
            query = f""" SELECT request_id, url, scan_profile FROM `hbbtv.requests` WHERE url LIKE r'%{value}%'; """

            syncing_candidates = exec_select_query(query)
        except Exception as e:
            print(value, e)


        # We have three possible options
        if len(syncing_candidates) == 0:
            # No cookie syncing candidate found
            c_no += 1
            #print("No syncing candidates")
        elif len(syncing_candidates) == 1:
            # One POSSIBLE syncing candidate
            c_possible += 1

            request_id = syncing_candidates['request_id'].tolist()[0]
            url = syncing_candidates['url'].tolist()[0]
            scan_profile = syncing_candidates['scan_profile'].tolist()[0]

            print(f"DEBUG: (dst)request_id=%d scan_profile=%d origin=%s name=%s value=%s url=%s (src)request_id=%d" % (request_id, scan_profile, origin, name, value, url, src_req_id))
            update_syncing_table(scan_profile, origin, name, value, src_req_id, request_id, url)

            # Check if the syncing candidate has the same party. Should return one row!
            #query_syncing_candidate = f""" SELECT c.origin, c.name, c.value
            #                                FROM `hbbtv.cookies` c, `hbbtv.requests`
            #                                WHERE c.request_id = {request_id}
            #                                AND c.scan_profile = {scan_profile}"""

            #candidates = exec_select_query(query_syncing_candidate)

            #if len(candidates) > 0:
                # Get the origin, name and value of the candidate
            #    c_origin = candidates['origin'].tolist()[0]
            #    c_name = candidates['name'].tolist()[0]
            #    c_value = candidates['value'].tolist()[0]

            #    if origin != c_origin and name != c_name and value != value:
            #        print(f"DEBUG: request_id = %d scan_profile = %d" % (request_id, scan_profile))
            #        c_possible += 1


        elif len(syncing_candidates) > 1:
            # Multiple POSSIBLE syncing candidate
            c_possible += 1
            for index, candidate in syncing_candidates.iterrows():
                request_id = candidate['request_id']
                url = candidate['url']
                scan_profile = candidate['scan_profile']
                print(f"DEBUG: (dst)request_id=%d scan_profile=%d origin=%s name=%s value=%s url=%s (src)request_id=%d" % (request_id, scan_profile, origin, name, value, url, src_req_id))
                update_syncing_table(scan_profile, origin, name, value, src_req_id, request_id, url)



    print(f"No: {c_no}\nPossible Syncing: {c_possible}")

def check_entropy(value, min_entropy=8):
    """
    Check the entropy of a cookie value.

    :param min_entropy: minium entropy the cookie value needs to have
    :param value: cookie value to check
    :return: true if the value passes the entropy check
    """
    return len(value.encode('utf-8')) >= min_entropy



def validate_epoch_timestamp(timestamp):
    try:
        timestamp_int = int(timestamp)
        #print(0 <= timestamp_int <= int(time.time())*1000, timestamp_int, int(time.time())*1000)
        return 0 <= timestamp_int <= int(time.time())*1000
    except Exception:
        return False

def clear_value(origin, name, scan_profile, value):
    """
    Eliminate values based on filtering methods like:
    1.

    :param origin:
    :param name:
    :param scan_profile:
    :param value:
    :return:
    """
    query = """ SELECT value, scan_profile FROM `hbbtv.cookies`
                WHERE origin=\'%s\' AND name=\'%s\' """ % (origin, name)

    result_df_cookies = exec_select_query(query)

    eliminated = False

    # Check if length of any value is different to the original value
    for index, row in result_df_cookies.iterrows():
        sp = row['scan_profile']
        v = row['value']
        v_len = len(v)

        if v_len != len(value):
            break
            eliminated = True

    # Check Ratcliff pattern
    #similarity_ratio = SM(isjunk=None, value, ).ratio()

    # Return value with smiliarity ratio greater than 90%
    #if similarity_ratio >= 0.9 and not eliminated:
    #    return value, True
    #else:
    #    return value, False

    return not eliminated


def update_syncing_table(scan_profile, origin, name, value, request_id_cookie, request_id, url):
    """
    """
    print("\nUPDATE SYNCING TABLE")
    src = request_id_cookie
    dst = request_id

    query = f""" INSERT INTO `hbbtv-research.hbbtv.Cookie_Syncing`
                VALUES ({scan_profile}, r'{value}', r'{name}',r'{origin}',
                {src}, {dst}, r'{url}'); """

    print(insertInto(query))

def get_cookie_values():
    """
    Get all distinct cookie values from table cookies.

    :return: list of cookie values with entropy higher or equal and list with cookie value, name and origin
    """
    all_distinct_cookie_values = exec_select_query(f""" SELECT distinct c. origin, c.name, c.value, c.scan_profile, c.request_id
                                                        FROM `hbbtv.cookies` c
                                                        WHERE BYTE_LENGTH(c.value) > 10
                                                        AND BYTE_LENGTH(c.value) < 25; """)

    cookies = list()

    timestamp_pattern = re.compile(r'\b(\d{13,})\b')

    # Get all cookies with entropy six or higher for further analyze
    for index, row in all_distinct_cookie_values.iterrows():
        origin = row['origin']
        name = row['name']
        value = row['value']
        scan_profile = row['scan_profile']
        r_id = row['request_id']

        match = timestamp_pattern.findall(value)

        if len(match) > 0:
            if validate_epoch_timestamp(match[0]):
                # We dont add the value
                pass
            else:
                is_valid = clear_value(origin, name, scan_profile, value)
                if is_valid:
                    query = f""" SELECT request_id, url
                                FROM `hbbtv.requests`
                                WHERE scan_profile={scan_profile}
                                AND is_first_party=True
                                AND url LIKE r'%{value}%'; """


                    result_df = exec_select_query(query)

                    if len(result_df) == 0:
                        # No candidates found
                        #print("No candidate found")
                        pass
                    elif len(result_df) == 1:
                        # One possible candidate
                        request_id = result_df['request_id'].tolist()[0]
                        url = result_df['url'].tolist()[0]

                        print("Single", origin, name, value, scan_profile, request_id, url)
                    elif len(result_df) > 1:
                        # Multiple syncing candidates
                        for index, row in result_df.iterrows():
                            request_id = result_df['request_id'].tolist()[0]
                            url = result_df['url'].tolist()[0]
                            print("Multiple:", origin, name, value, scan_profile, request_id, url)


if __name__ == '__main__':
    #get_cookie_values()
    analyze_http_requests()
    #test_analyze()

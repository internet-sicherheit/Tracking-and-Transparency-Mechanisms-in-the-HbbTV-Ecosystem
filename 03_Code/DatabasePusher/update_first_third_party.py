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
from collections.abc import Iterable
from tqdm import tqdm

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


def get_blacklist():
    """
    Gets a blacklist for etld which cannot be a third party!

    :Returns: dict with filter
    """
    with open("first_third_party_blacklist.txt", "r") as f:
        json_obj = json.loads(f.readline())

    blacklist = list()
    for el in json_obj:
        blacklist.append(list(el.values())[0])

    return blacklist


def set_first_and_third_party(profile):
    """
    Set the first and third party for the given profile

    :profile: Given profile to identify first and third party
    :Return: nothing, Prints out to standard output
    """

    result_df = exec_select_query(f""" SELECT req.channelid, etld, MIN(req.time_stamp) AS time_stamp
                                        FROM
                                          `hbbtv-research.hbbtv.requests` req
                                        JOIN
                                          `hbbtv-research.hbbtv.responses` res
                                        ON
                                          req.request_id=res.request_id
                                        WHERE
                                          res.type LIKE '%html%'
                                          AND req.method="GET"
                                          AND req.scan_profile={profile}
                                          AND req.request_id = res.request_id
                                        GROUP BY
                                          req.channelid, etld
                                        ORDER BY
                                          req.channelid,
                                          MIN(req.time_stamp)""")

    blacklist = get_blacklist()

    first_party = dict() # contains all first party -> first requests which response with a HTML
    thrid_party = dict()


    # Set the first party
    for index, row in tqdm(result_df.iterrows(), desc="Identify the first party"):
        channel_id = row['channelid']
        etld = row['etld']
        ts = row['time_stamp']

        t = (etld, ts)
        if list(first_party.keys()).count(channel_id) == 0:
            if etld in blacklist: # check if etld is a possible first party
                first_party[channel_id] = t
        else:
            # Check if timestamp from the current value is higher than the new
            old_ts = first_party[channel_id][1]
            if old_ts > ts :
                if etld in blacklist: # check if etld is a possible first party
                    first_party[channel_id] = t # if true, set a new value (etld, timestamp)


    r_id = list()
    for k,v in tqdm(first_party.items(), desc="Get Request IDs"):
        channel_id = k
        etld = v[0]
        result_df_request_ids = exec_select_query(f""" SELECT request_id
                                                        FROM `hbbtv-research.hbbtv.requests`
                                                        WHERE scan_profile={profile}
                                                        AND channelid=\"{channel_id}\"
                                                        AND etld=\"{etld}\" """)

        request_ids = result_df_request_ids['request_id'].tolist()

        r_id.append(request_ids)


        #update_first_third_party(request_ids, profile, True, False, True)

    return r_id


def update_first_third_party(request_ids, scan_profile, first_party, third_party, update=False):
    """
    Updates the first and third-party fields for the first party in the request and response table.

    :param request_id: The request ID of the request to update.
    :param scan_profile: The scan profile of the request to update.
    :param first_party: The first party of the request to update.
    :param third_party: The third party of the request to update.
    :param update: Simple flag for debugging reason to indicate if we found a new channel.
    :return: None
    """

    if update:
        update_requests_query = f""" UPDATE `hbbtv.requests`
                               SET is_first_party={first_party}, is_third_party={third_party}
                               WHERE scan_profile={scan_profile}
                               AND request_id IN UNNEST ({request_ids}) ;"""

        exec_update_query(update_requests_query)

        update_response_query = f""" UPDATE `hbbtv.responses`
                               SET is_first_party={first_party}, is_third_party={third_party}
                               WHERE scan_profile={scan_profile}
                               AND request_id IN UNNEST ({request_ids}) ;"""

        exec_update_query(update_response_query)

    else:
        update_count = exec_select_query(f"""SELECT COUNT(*) AS count
                                            FROM `hbbtv.requests`
                                            WHERE scan_profile={scan_profile}
                                            AND request_id IN UNNEST ({request_ids});""")

        affected_rows = update_count.iloc[:1]['count'][0]
        if affected_rows != 1:
            print("I would have updated %d rows! Aborting. Debug: request=%s profile=%d" % (
                affected_rows, request_ids, scan_profile))
            return
        else:
            print("Updating! Debug: request=%s profile=%d" % (request_ids, scan_profile))

def update_third_party(scan_profile, first_party=False, third_party=True, update=False):
    """
    Updates the first and third-party fields in the request and response table.

    :param request_id: The request ID of the request to update.
    :param scan_profile: The scan profile of the request to update.
    :param first_party: The first party of the request to update.
    :param third_party: The third party of the request to update.
    :param update: Simple flag for debugging reason to indicate if we found a new channel.
    :return: None
    """
    result_df_request_ids = exec_select_query(f""" SELECT request_id
                                                    FROM `hbbtv.requests`
                                                    WHERE scan_profile={scan_profile}
                                                    AND is_first_party IS NULL
                                                    AND is_third_party IS NULL; """)

    request_ids = result_df_request_ids['request_id'].tolist()

    if update:
        update_requests_query = f""" UPDATE `hbbtv.requests`
                               SET is_first_party={first_party}, is_third_party={third_party}
                               WHERE scan_profile={scan_profile}
                               AND request_id IN UNNEST ({request_ids}) ;"""

        exec_update_query(update_requests_query)

        update_response_query = f""" UPDATE `hbbtv.responses`
                               SET is_first_party={first_party}, is_third_party={third_party}
                               WHERE scan_profile={scan_profile}
                               AND request_id IN UNNEST ({request_ids}) ;"""

        exec_update_query(update_response_query)
    else:
        update_count = exec_select_query(f"""SELECT COUNT(*) AS count
                                            FROM `hbbtv.requests`
                                            WHERE scan_profile={scan_profile}
                                            AND request_id IN UNNEST ({request_ids});""")

        affected_rows = update_count.iloc[:1]['count'][0]
        if affected_rows != 1:
            print("I would have updated %d rows! Aborting. Debug: request=%d profile=%d" % (
                affected_rows, len(request_ids), scan_profile))
            return
        else:
            print("Updating! Debug: request=%s profile=%d" % (request_ids, scan_profile))

def flatten_inhomogeneous_array(arr):
    for elem in arr:
        if isinstance(elem, Iterable) and not isinstance(elem, (str, bytes)):
            yield from flatten_inhomogeneous_array(elem)
        else:
            yield elem

if __name__ == '__main__':
    request_ids = list()

    print(f"Update profile {1}")
    request_ids.append(set_first_and_third_party(1))

    print(f"Update profile {3}")
    request_ids.append(set_first_and_third_party(3))

    print(f"Update profile {4}")
    request_ids.append(set_first_and_third_party(4))

    #update_third_party(4, False, True, True)

    print(f"Update profile {5}")
    request_ids.append(set_first_and_third_party(5))
    #update_third_party(5, False, True, True)

    print(f"Update profile {6}")
    request_ids.append(set_first_and_third_party(6))



    rqid = flattened_array = list(flatten_inhomogeneous_array(request_ids))

    # rqid = list(set(rqid))
    #print(rqid, len(rqid))

    result_df = exec_select_query(f""" SELECT distinct channelname, url FROM hbbtv.requests WHERE request_id IN UNNEST ({rqid}); """)

    result_df.to_csv("channelname_url_first_party.csv", index=False)

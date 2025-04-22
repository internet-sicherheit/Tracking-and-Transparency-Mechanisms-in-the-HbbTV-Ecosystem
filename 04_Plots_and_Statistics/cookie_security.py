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
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "resources", "google_bkp.json")
    client = bigquery.Client()

    # Execute the query and retrieve result data (as pandas dataframe)
    result_df = client.query(query).to_dataframe()

    return result_df


def get_cookie_security_flags():
    """
    Distribution of security flags.

    :return:
    """
    result_df_secure = exec_select_query(f""" SELECT foo.secure, count(*)
                                            FROM (SELECT distinct origin, name, secure, sameSite1, http_only
                                            FROM `hbbtv-research.hbbtv.cookies`)
                                            AS foo GROUP BY foo.secure; """)

    result_df_http_only = exec_select_query(f""" SELECT foo.http_only, count(*)
                                                FROM (SELECT distinct origin, name, secure, sameSite1, http_only
                                                FROM `hbbtv-research.hbbtv.cookies`) AS foo GROUP BY foo.http_only;""")

    result_df_sameSite = exec_select_query(f""" SELECT foo.sameSite1, count(*)
                                                FROM (SELECT distinct origin, name, secure, sameSite1, http_only
                                                FROM `hbbtv-research.hbbtv.cookies`) AS foo GROUP BY foo.sameSite1; """)

    data_df = pd.DataFrame({'Security_flag': ['secure', 'httpOnly', 'sameSite']})

    secure = result_df_secure['f0_'].tolist() # pos 0: false, pos 1:true
    http_only = result_df_http_only['f0_'].tolist() # pos 0: false, pos 1:true
    sameSite = result_df_sameSite['f0_'].tolist() # pos 0: false, pos 1:true

    data_df = data_df.assign(Set=[secure[1],http_only[1],sameSite[1]])
    data_df = data_df.assign(Not_Set=[secure[0],http_only[0],sameSite[0]])


    print(data_df.to_latex(index=False))

    data_df.plot(x="Security_flag", y=["Set", "Not_Set"], kind="bar")
    #plt.show()
    plt.savefig("Cookie_Security.pdf", format="pdf", bbox_inches="tight")

if __name__ == '__main__':
    get_cookie_security_flags()

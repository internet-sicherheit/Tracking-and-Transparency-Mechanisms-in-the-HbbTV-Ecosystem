import os
import sqlite3
from adblockparser import AdblockRules
from tqdm import tqdm
from google.cloud import bigquery
from whotracksme.data.loader import DataSource

def exec_select_query(query):
    """
    Executes the given SQL query using the static Google authentication credentials.

    :param query: The SQL query
    :return: A (pandas) dataframe that contains the results
    """
    # Initialize teh Google BigQuery client. The authentication token should be placed in the working directory in the
    # following path: /resources/google.json
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "..", "DatabasePusher", "resources", "google_bkp.json")
    client = bigquery.Client()

    # Execute the query and retrieve result data (as pandas dataframe)
    result_df = client.query(query).to_dataframe()

    return result_df


def exec_update_query(query):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "..", "DatabasePusher", "resources", "google_bkp.json")
    client = bigquery.Client()

    result = client.query(query)

    return "Total rows updated: ", str(result.num_dml_affected_rows)

def update_easylist_privacy():
    """
    Update the request based on the easylist privacy list.
    """

    rules = init_adblocker()

    # data = DataSource()
    #
    # trackers = data.trackers
    #
    # for tracker in list(trackers:
    #     print(tracker)
    #
    # exit()

    #request_to_update = set

    # Get all requests
    result_df = exec_select_query("""SELECT request_id, scan_profile, etld, url
                                        FROM `hbbtv.requests`;""")

    # Iterate over requests and check if they would have been blocked by the given block list
    for row in tqdm(result_df.iterrows(), desc="Requests"):
        req_id = row[1]['request_id']
        scan_id = row[1]['scan_profile']
        etld = row[1]['etld']
        url = row[1]['url']

        # Test if etld is on the block list
        if is_known_blocker(etld, rules):
            #print("is blocked:", url)
            #request_to_update.add((req_id, scan_id))
            # TODO update biqquerydb
            query = f""" UPDATE `hbbtv.requests` SET is_easylist_privacy_blocked=True WHERE request_id={req_id} AND scan_profile={scan_id} """
            #print(query)

            exec_update_query(query)


def init_adblocker():
    """
    Initialize the adblock parser.

    :return: The parser object
    """
    with open(os.path.join('resources', 'easylist_privacy.txt'), encoding="utf-8") as f:
        raw_rules = f.read().splitlines()
    return AdblockRules(raw_rules)

def is_known_blocker(url, rules):
    """
    Tests if the given URL is present on the initialized block list.

    :param url: The URL to test
    :param rules: The parser object
    :return: TRUE if present on the list
    """
    return rules.should_block(url)



if __name__ == '__main__':
    update_easylist_privacy()

import os
from google.cloud import bigquery
from google.cloud.bigquery import enums

# Directory that contains the blocklist
DATA_DIR = "resources"

# Name of the file that contains the blocklist
PIHOLE_FILE_NAME = "pihole_hosts.txt"

SPECIFIC_PIHOLE_FILE_NAME = ""

SMARTTV_FILE_NAME_TWO = "smart-tv-2.txt"
SMARTTV_FILE_NAME = "smart-tv.txt"


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


def read_pihole_urls():
    """
    Reads PiHole rules from file.
    :return: A set containing the filter rules
    """
    url_ruleset = set()

    # Read blocking rules from file
    with open(os.path.join(DATA_DIR, SPECIFIC_PIHOLE_FILE_NAME)) as pihole_file:
        while line := pihole_file.readline():
            line = line.rstrip()
            # Skip comments and empty lines
            if line.startswith("#") or len(line) == 0:
                continue
            # Add the URLs (eTLD+1) for each domain
            url_ruleset.add(line.split(' ')[1])

    return url_ruleset


def read_specific_rules_for_smarttv():
    url_ruleset = set()

    # Read blocking rules from file
    with open(os.path.join(DATA_DIR, SMARTTV_FILE_NAME_TWO)) as pihole_file:
        while line := pihole_file.readline():
            line = line.rstrip()
            # Skip comments and empty lines
            if line.startswith("#") or len(line) == 0:
                continue
            # Add the URLs (eTLD+1) for each domain
            print(line)
            url_ruleset.add(line)

    return url_ruleset


def update_big_query_database(rule_set):
    """
    Updates the BigQuery database to indicate if a request would have been blocked by teh given filter
    list (i.e., PiHole list).
    :param rule_set: A blocklist (set) of eTLDs
    :return: None
    """

    request_to_update = set()

    # Get all requests
    result_df = exec_select_query("""SELECT request_id, scan_profile, etld, url
                                        FROM `hbbtv.requests`;""")

    # Iterate over requests and check if they would have been blocked by the given block list
    for row in result_df.iterrows():
        req_id = row[1]['request_id']
        scan_id = row[1]['scan_profile']
        etld = row[1]['etld']
        url = row[1]['url']

        # Test if etld is on the block list
        if etld in rule_set:
            print("is blocked:", url)
            request_to_update.add((req_id, scan_id))
            # TODO update biqquerydb
            print(exec_update_query(f""" UPDATE `hbbtv.requests` SET is_smart_tv_blocked_list_2=True WHERE request_id={req_id} AND scan_profile={scan_id} """))

    #print(request_to_update)
    #exec_update_query(f""" UPDATE `hbbtv.requests` SET pi_hole_blocked=True WHERE (request_id, scan_profile) IN UNNEST({request_to_update}) """)
    #print(f"Updated {len(request_to_update)} requests")


if __name__ == '__main__':
    # Get rules
    #blocked_urls = read_pihole_urls()
    blocked_urls = read_specific_rules_for_smarttv()

    # Update BigQuery database
    update_big_query_database(blocked_urls)

import os
import pandas as pd
from google.cloud import bigquery
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


def correlation_profile_http_cookies():
    """
    Computes correlation on the effect of the different profiles on HTTP(s) traffic and cookie usage.

    :return: None
    """
    # scan profile ~ http traffic
    result_df = exec_select_query("""SELECT scan_profile, channelname, COUNT (*) AS num_http_req
                                        FROM `hbbtv-research.hbbtv.requests`
                                        GROUP BY scan_profile, channelname;""")

    grouped_df = result_df.groupby('scan_profile')
    df1 = grouped_df.get_group(1)['num_http_req']  # normal
    df3 = grouped_df.get_group(3)['num_http_req']  # red
    df4 = grouped_df.get_group(4)['num_http_req']  # yellow
    df5 = grouped_df.get_group(5)['num_http_req']  # blue
    df6 = grouped_df.get_group(6)['num_http_req']  # green

    kruskal_profile_http = stats.kruskal(df1, df3, df4, df5, df6)
    n = len(result_df)
    print("kruskal: scan profile ~ http traffic (p-value): ", kruskal_profile_http[1], "eta square:",
          compute_eta_square(kruskal_profile_http[0], 5, n))

    #scan profile ~ cookie usage
    result_df = exec_select_query("""SELECT req.scan_profile, req.channelname, COUNT (*) AS num_cookies_set
                                        FROM `hbbtv-research.hbbtv.cookies` AS c, `hbbtv-research.hbbtv.requests` AS req
                                        WHERE NOT c.duplicate AND c.scan_profile = req.scan_profile AND c.request_id = req.request_id
                                        GROUP BY scan_profile, channelname;""")

    grouped_df = result_df.groupby('scan_profile')
    df1 = grouped_df.get_group(1)['num_cookies_set']  # normal
    df3 = grouped_df.get_group(3)['num_cookies_set']  # red
    df4 = grouped_df.get_group(4)['num_cookies_set']  # yellow
    df5 = grouped_df.get_group(5)['num_cookies_set']  # blue
    df6 = grouped_df.get_group(6)['num_cookies_set']  # green

    kruskal_cookies = stats.kruskal(df1, df3, df4, df5, df6)
    n = len(result_df)
    print("kruskal: scan profile ~ cookies set (p-value): ", kruskal_cookies[1], "eta square:",
          compute_eta_square(kruskal_cookies[0], 5, n))


def high_level_numbers():
    """
    Computes some very high level numbers on our experiment.

    :return: None
    """

    # How many channels did we analyze?
    result_df_channels = exec_select_query("""SELECT COUNT(DISTINCT req.channelid)
                                                FROM `hbbtv-research.hbbtv.requests` AS req;""")
    print("[GO1.1] number of analyzed channels: ", result_df_channels)

    # Channel by satellite distribution?
    result_df_satellites = exec_select_query("""SELECT satelliteName, COUNT (DISTINCT channelid)
                                                FROM `hbbtv-research.hbbtv.channellist`
                                                GROUP BY satelliteName;""")
    print("[GO1.2] distribution of of analyzed channels: ", result_df_satellites)

    # How many requests did we analyze?
    result_df_http = exec_select_query("""SELECT COUNT(*) AS http_requests
                                                FROM `hbbtv-research.hbbtv.requests` AS req;""")
    print("[GO1.3] number http(s) requests: ", result_df_http)

    # How many cookies were used?
    result_df_cookies = exec_select_query("""SELECT storage_type, COUNT(*)
                                            FROM `hbbtv-research.hbbtv.tv_cookie_store`
                                            GROUP BY storage_type;""")
    print("[GO1.4] number http(s) requests: ", result_df_cookies)

    # How often did we interact with the TV?
    result_df_cookies = exec_select_query("""SELECT COUNT(*) AS interactions
                                                FROM `hbbtv-research.hbbtv.LogData`
                                                WHERE command IN ('set_channel_with_id', 'ok', 'up', 'red', 'blue',
                                                    'down', 'left', 'green', 'right', 'yellow');""")
    print("[GO1.5] number on interactions: ", result_df_cookies)

    # How many screenshots did we take?
    result_df_cookies = exec_select_query("""SELECT COUNT(*) AS screenshots
                                                FROM `hbbtv-research.hbbtv.LogData`
                                                WHERE command ='screenshot';""")
    print("[GO1.6] number of screenshots: ", result_df_cookies)


def measurement_overview():
    """
    Computes a general overview of the measurement results.
    Prints the general overview table.

    :return: None
    """
    # Measurement dates
    date_df = pd.DataFrame({'scan_profile': [1, 3, 4, 5, 6], 'date': ['08/21/23', '09/14/23', '10/12/23', '09/27/23', '09/22/23']})

    # Number of analyzed channels
    result_df_channels = exec_select_query("""SELECT scan_profile, COUNT (DISTINCT req.channelid) AS number_channels
                                                FROM `hbbtv-research.hbbtv.requests` AS req
                                                GROUP BY scan_profile ;""")
    merged_df = pd.merge(date_df, result_df_channels, on='scan_profile')

    # Volume of HTTP(s) traffic
    result_df_http_all = exec_select_query("""SELECT req.scan_profile, COUNT (*) AS http_requests
                                        FROM `hbbtv-research.hbbtv.requests` AS req
                                        GROUP BY scan_profile;""")
    # result_df_http = exec_select_query("""SELECT req.scan_profile, COUNT (*) AS http_requests
    #                                     FROM `hbbtv-research.hbbtv.requests` AS req
    #                                     WHERE url LIKE 'http://%'
    #                                     GROUP BY scan_profile;""")

    result_df_https = exec_select_query("""SELECT req.scan_profile, COUNT (*) AS https_requests
                                        FROM `hbbtv-research.hbbtv.requests` AS req
                                        WHERE url LIKE 'https://%'
                                        GROUP BY scan_profile;""")
    merged_df = pd.merge(merged_df, result_df_http_all, on='scan_profile')
    merged_df = pd.merge(merged_df, result_df_https, on='scan_profile')

    merged_df['https_share'] = (merged_df['https_requests'] / merged_df['http_requests'] * 100)
    merged_df['https_share'] = merged_df['https_share'].map('{:.2f}%'.format)
    merged_df['http_requests'] = merged_df['http_requests'].map('{:,}'.format)
    merged_df['https_requests'] = merged_df['https_requests'].map('{:,}'.format)

    # Cookie usage
    result_df_all_cookies = exec_select_query("""SELECT foo.scan_profile, COUNT(*) AS all_cookies
                                                    FROM (SELECT DISTINCT c.name, c.origin, c.scan_profile
                                                          FROM `hbbtv-research.hbbtv.cookies` AS c
                                                          WHERE NOT duplicate) AS foo
                                                    GROUP BY foo.scan_profile;""")

    result_df_all_first_party = exec_select_query("""SELECT foo.scan_profile, COUNT(*) AS first_party_cookies
                                                        FROM (SELECT DISTINCT c.name, c.origin, c.scan_profile
                                                              FROM`hbbtv-research.hbbtv.cookies` AS c, `hbbtv-research.hbbtv.requests` AS req
                                                              WHERE NOT c.duplicate AND req.scan_profile = c.scan_profile
                                                                  AND req.request_id = c.request_id AND req.is_first_party) AS foo
                                                        GROUP BY foo.scan_profile;""")

    result_df_all_third_party = exec_select_query("""SELECT foo.scan_profile, COUNT(*) AS third_party_cookies
                                                        FROM (SELECT DISTINCT c.name, c.origin, c.scan_profile
                                                              FROM`hbbtv-research.hbbtv.cookies` AS c, `hbbtv-research.hbbtv.requests` AS req
                                                              WHERE NOT c.duplicate AND req.scan_profile = c.scan_profile
                                                                  AND req.request_id = c.request_id AND NOT req.is_first_party) AS foo
                                                        GROUP BY foo.scan_profile;""")
    # print(result_df_all_third_party)
    merged_df = pd.merge(merged_df, result_df_all_cookies, on='scan_profile')
    merged_df = pd.merge(merged_df, result_df_all_first_party, on='scan_profile')
    merged_df = pd.merge(merged_df, result_df_all_third_party, on='scan_profile')
    merged_df['all_cookies'] = merged_df['all_cookies'].map('{:,}'.format)
    merged_df['first_party_cookies'] = merged_df['first_party_cookies'].map('{:,}'.format)
    merged_df['third_party_cookies'] = merged_df['third_party_cookies'].map('{:,}'.format)

    # Local storage usage
    result_df_local_storage = exec_select_query("""SELECT foo.scan_profile, COUNT(*) AS number_local_storage_cookies
                                                    FROM (SELECT DISTINCT c.name, c.host_key, c.scan_profile
                                                          FROM `hbbtv-research.hbbtv.tv_cookie_store` as c
                                                          WHERE c.storage_type = 'local storage') AS foo
                                                    GROUP BY foo.scan_profile;""")
    result_df_local_storage['scan_profile'] = result_df_local_storage['scan_profile'].astype(int)
    merged_df = pd.merge(merged_df, result_df_local_storage, on='scan_profile')
    merged_df['number_local_storage_cookies'] = merged_df['number_local_storage_cookies'].map('{:,}'.format)

    # Sort and include profile names
    merged_df = merged_df.sort_values(by=['scan_profile'])
    merged_df['scan_profile'] = merged_df['scan_profile'].map({1: 'General', 3: 'Red', 4: 'Yellow', 5: 'Blue', 6: 'Green'})

    # Print table
    print(merged_df.to_latex(index=False))


if __name__ == '__main__':
    # high_level_numbers()
    measurement_overview()
    # correlation_profile_http_cookies()

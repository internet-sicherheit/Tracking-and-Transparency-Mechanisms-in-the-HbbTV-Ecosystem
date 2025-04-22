from scipy import stats
import pandas as pd
from google.cloud import bigquery
import os
import sys

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


def tracking_requests_on_channels():
    result_df = exec_select_query("""SELECT
                                      channelid,
                                      scan_profile,
                                      COUNT(*) tracker
                                    FROM
                                      hbbtv.requests
                                    WHERE
                                      (is_known_tracker
                                      OR is_fingerprint
                                      OR is_tracking_pixel)
                                    GROUP BY
                                      channelid,
                                      scan_profile; """)


    channellist = list(set(result_df['channelid'].tolist()))

    grouped_df = result_df.groupby('channelid')
    df_list = list()

    for ch in channellist:
        df = grouped_df.get_group(ch)['tracker']
        df_list.append(df)



    kruskal_channel_id = stats.kruskal(*df_list)
    n = len(result_df)
    print("Kruskal: channel id ~ tracker:", kruskal_channel_id[1], "eta square:", compute_eta_square(kruskal_channel_id[0], len(channellist), n))

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

def tracker_profile():
    result_df = exec_select_query("""SELECT
                                      channelid,
                                      scan_profile,
                                      COUNT(*) tracker
                                    FROM
                                      hbbtv.requests
                                    WHERE
                                      (is_known_tracker
                                      OR is_fingerprint
                                      OR is_tracking_pixel)
                                    GROUP BY
                                      channelid,
                                      scan_profile; """)


    channellist = list(set(result_df['channelid'].tolist()))

    grouped_df = result_df.groupby('channelid')

    df_stats = list()
    for ch in channellist:
        df = grouped_df.get_group(ch)
        df_stats.append({'channel_id':ch, 'tracker':df['tracker'].sum()})

    df_stats = pd.DataFrame.from_dict(df_stats)

    print("[CA.1.0] tracker on channel mean:", df_stats['tracker'].mean(), "min:",df_stats['tracker'].min() ,"max:", df_stats['tracker'].max(),"sd:",df_stats['tracker'].std())

    grouped_df = result_df.groupby('scan_profile')
    df1 = grouped_df.get_group(1)['tracker']
    df3 = grouped_df.get_group(3)['tracker']
    df4 = grouped_df.get_group(4)['tracker']
    df5 = grouped_df.get_group(5)['tracker']
    df6 = grouped_df.get_group(6)['tracker']

    print(df1.tolist().count(0), len(df1.tolist()))

    print("[CA.1.1] tracker per channel on profile 1 mean:", df1.mean(), "min:", df1.min(), "max:", df1.max(), "sd:", df1.std())
    print("[CA.1.2] tracker per channel on profile 3 mean:", df3.mean(), "min:", df3.min(), "max:", df3.max(), "sd:", df3.std())
    print("[CA.1.3] tracker per channel on profile 4 mean:", df4.mean(), "min:", df4.min(), "max:", df4.max(), "sd:", df4.std())
    print("[CA.1.4] tracker per channel on profile 5 mean:", df5.mean(), "min:", df5.min(), "max:", df5.max(), "sd:", df5.std())
    print("[CA.1.5] tracker per channel on profile 6 mean:", df6.mean(), "min:", df6.min(), "max:", df6.max(), "sd:", df6.std())


def tracker_on_channels():
    result_df = exec_select_query("""
                                    SELECT
                                      channelid,
                                      etld as url
                                    FROM
                                      hbbtv.requests
                                    WHERE
                                      (is_known_tracker
                                        OR is_fingerprint
                                        OR is_tracking_pixel);
                                     """)

    df_grouped = result_df.groupby('channelid')
    chid = list(set(result_df['channelid'].tolist()))

    l = list()
    for ch in chid:
        df = df_grouped.get_group(ch)
        distinct_tracker = len(list(set(df['url'].tolist())))
        l.append({"channelid": ch, "tracker": distinct_tracker, "requests": len(df['url'].tolist())})

    df = pd.DataFrame(l)
    df = df.sort_values(by='tracker', ascending=False)
    print("[CA.2.0] tracker on each channel: mean",df['tracker'].mean(),"min:",df['tracker'].min(),"max:", df['tracker'].max(),"SD:", df['tracker'].std())

    total_traffic = df['requests'].sum()
    traffic_top_10 = df.head(10)['requests'].sum()

    print("[CA.2.1] percentage of requests from the top 10 tracker:", round(traffic_top_10/total_traffic*100,2), "%")


if __name__ == '__main__':
    # tracking_requests_on_channels()
    # tracker_profile()
    tracker_on_channels()

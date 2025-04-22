from google.cloud import bigquery
import os
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sb
import pandas as pd
import numpy as np
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

def third_parties_cookie_usage():
    """
    Computes the statistically significans of the volume of third-party cookie usage across the profiles.

    :return: None
    """
    result_df_cookies = exec_select_query(""" SELECT scan_profile, channelid, count(*) as count
                                                FROM `hbbtv-research.hbbtv.requests`
                                                WHERE is_first_party=True
                                                group by scan_profile,  channelid
                                                order by scan_profile """)

    grouped_df = result_df_cookies.groupby('scan_profile')
    df1 = grouped_df.get_group(1)['count']  # normal
    df3 = grouped_df.get_group(3)['count']  # red
    df4 = grouped_df.get_group(4)['count']  # yellow
    df5 = grouped_df.get_group(5)['count']  # blue
    df6 = grouped_df.get_group(6)['count']  # green

    kruskal_profile_cookie_usage = stats.kruskal(df1, df3, df4, df5, df6)
    n = len(result_df_cookies)

    print(f"[Cookies.5.X] Kruskal: scan profile ~ third party usage (p-value) {kruskal_profile_cookie_usage[1]}, eta square {compute_eta_square(kruskal_profile_cookie_usage[0], 5, n)}")

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


def plot_third_party_distribution():
    """
    Computes the plot that shows the distribution of third parties that use cookies and writes it to disk.
    :return: None
    """
    # Set general plot styles
    matplotlib.rcParams['pdf.fonttype'] = 42
    matplotlib.rcParams['ps.fonttype'] = 42
    matplotlib.rcParams['text.usetex'] = False
    matplotlib.rcParams['axes.labelweight'] = 'bold'
    sb.set(rc={'figure.figsize': (13, 4.2), "font.size": 18, "axes.titlesize": 18, "axes.labelsize": 18,
           "legend.fontsize": 18, "xtick.labelsize": 18, "ytick.labelsize": 18}, style="white")
    sb.set_context('paper', font_scale=3.5)

    # Get the data
    result_df_cookie_usage = exec_select_query(f"""SELECT foo.origin AS origin, COUNT(*) AS count
                                                    FROM (SELECT DISTINCT c.origin, req.channelid
                                                      FROM
                                                        `hbbtv-research.hbbtv.requests` req,
                                                        `hbbtv-research.hbbtv.cookies` c
                                                      WHERE req.request_id=c.request_id
                                                        AND req.scan_profile=c.scan_profile
                                                        AND NOT c.duplicate
                                                        AND req.is_third_party) AS foo
                                                    GROUP BY foo.origin
                                                    ORDER BY COUNT(*) DESC;""")

    result_df_cookie_usage_greater_10 = exec_select_query(""" SELECT * FROM (
                                                            SELECT
                                                              foo.origin AS origin,
                                                              COUNT(*) AS count
                                                            FROM (
                                                              SELECT
                                                                DISTINCT c.origin,
                                                                req.channelid
                                                              FROM
                                                                `hbbtv-research.hbbtv.requests` req,
                                                                `hbbtv-research.hbbtv.cookies` c
                                                              WHERE
                                                                req.request_id=c.request_id
                                                                AND req.scan_profile=c.scan_profile
                                                                AND NOT c.duplicate
                                                                AND req.is_third_party) AS foo
                                                            GROUP BY
                                                              foo.origin
                                                            ORDER BY
                                                              COUNT(*) DESC) WHERE count > 10; """)

    # Format the plot
    #g = sb.barplot(data=result_df_cookie_usage.head(96), x="origin", y="count", color='black', log=True)
    g = sb.barplot(data=result_df_cookie_usage, x="origin", y="count", color='black', log=True)

    g.set(xticklabels=[])
    g.tick_params(bottom=False)  # remove the ticks
    plt.xlabel("Third Party")
    plt.ylabel('Occurrence')
    plt.tight_layout()

    # Save plot
    plt.savefig(os.path.join(os.getcwd(), 'plots', 'p2_distribution_cookie_setting_third_parties.pdf'), dpi=600,
                transparent=False, bbox_inches='tight', format="pdf")

    # plt.savefig(os.path.join(os.getcwd(), 'plots', 'p2_distribution_cookie_setting_third_parties_reduced_singles.pdf'), dpi=600,
    #             transparent=False, bbox_inches='tight', format="pdf")


def cookie_usage_overview():
    """
    Provides the data for the chapter "Cookie Usage Overview"

    :return: None, prints out the statements
    """
    result_df_tv_export = exec_select_query(""" SELECT
                                              foo.storage_type,
                                              COUNT(*) AS count
                                            FROM (
                                              SELECT
                                                DISTINCT storage_type,
                                                name,
                                                path
                                              FROM
                                                `hbbtv-research.hbbtv.tv_cookie_store`) AS foo
                                            GROUP BY
                                              foo.storage_type
                                             ORDER BY
                                                foo.storage_type """)
    # distinct cookies in cookie jar (name, path, origin)
    cookie_jar = result_df_tv_export.iloc[0][1]
    print(f"[Q5.3.1.1] DISTINCT Cookies in cookie jar (name, path): {cookie_jar}")

    # distinct cookies in local storage (name, path, origin)
    local_storage = result_df_tv_export.iloc[0][1]
    print(f"[Q5.3.1.2] DISTINCT Cookies in local storage (name, path): {local_storage}")


    result_df_cookies_per_channel = exec_select_query(""" SELECT foo.channelname, count(*) as count FROM (SELECT
                                                          DISTINCT req.channelname,
                                                          c.origin,
                                                          c.name,
                                                          c.path
                                                        FROM
                                                          `hbbtv-research.hbbtv.requests` req,
                                                          `hbbtv-research.hbbtv.cookies` c
                                                        WHERE
                                                          req.request_id=c.request_id
                                                          AND req.scan_profile=req.scan_profile)
                                                          AS foo group by foo.channelname """)
    # Average cookies per channel
    sum = result_df_cookies_per_channel.sum()[1]
    n = len(result_df_cookies_per_channel)
    avg = int(sum/n)
    print(f"[Q5.3.1.3] AVG DISTINCT (name, origin, path) cookies per channel: {avg}")

    # Min/MAX cookies set on channel
    max_value = result_df_cookies_per_channel.loc[result_df_cookies_per_channel['count'].idxmax()][1]
    min_value = result_df_cookies_per_channel.loc[result_df_cookies_per_channel['count'].idxmin()][1]
    print(f"[Q5.3.1.4] MIN DISTINCT (name, origin, path) cookies per channel: {min_value}")
    print(f"[Q5.3.1.5] MAX DISTINCT (name, origin, path) cookies per channel: {max_value}")
    # SD for cookies on channel
    cookies_per_channel = result_df_cookies_per_channel['count'].tolist()
    sd = int(np.std(cookies_per_channel))

    print(f"[Q5.3.1.6] SD DISTINCT (name, origin, path) cookies on channel: {sd}")
    # Amount of classified cookies
    result_df_cookie_classified = exec_select_query(""" WITH
                                                          bar AS (
                                                          SELECT
                                                            foo.purpose,
                                                            COUNT(*) AS count
                                                          FROM (
                                                            SELECT
                                                              DISTINCT name,
                                                              path,
                                                              origin,
                                                              purpose
                                                            FROM
                                                              `hbbtv-research.hbbtv.cookies`) AS foo
                                                          GROUP BY
                                                            foo.purpose)
                                                        SELECT
                                                          foo.purpose,
                                                          SAFE_ADD(bar.count, foo.count)
                                                        FROM
                                                          bar,
                                                          (
                                                          SELECT
                                                            foo.purpose,
                                                            COUNT(*) AS count
                                                          FROM (
                                                            SELECT
                                                              DISTINCT storage_type,
                                                              name,
                                                              path,
                                                              purpose
                                                            FROM
                                                              `hbbtv-research.hbbtv.tv_cookie_store`) AS foo
                                                          GROUP BY
                                                            foo.purpose) AS foo
                                                        WHERE
                                                          foo.purpose = bar.purpose
                                                          order by foo.purpose """)

    unknown = result_df_cookie_classified.iloc[4][1]
    rest = 0
    for i in range(3):
        rest += result_df_cookie_classified.iloc[i][1]
    classified = "{:.0%}".format(rest/(unknown+rest))
    print(f"[Q5.3.1.7] Percentage of classified cookies: {classified}")


def cookie_overview(scan_profile):
    """

    USE SCRIPT IN BIQ QUERY!

    Computes the table for cookie overview in third party usage.

    :param scan_profile: Scan profile from the measurement
    :return: Table in LaTex Format
    """
    result_df_channels = exec_select_query(""" SELECT scan_profile, COUNT (DISTINCT req.channelid) AS number_channels
                                                FROM `hbbtv-research.hbbtv.requests` AS req
                                                GROUP BY scan_profile ORDER BY scan_profile ASC; """)
    # Get the data -                                                         AND c.scan_profile={scan_profile}
    result_df_cookie_usage = exec_select_query(f"""SELECT foo.scan_profile AS profile, foo.origin AS origin, COUNT(*) AS count
                                                    FROM (SELECT DISTINCT c.origin, c.scan_profile, req.channelname,
                                                      FROM
                                                        `hbbtv-research.hbbtv.requests` req,
                                                        `hbbtv-research.hbbtv.cookies` c
                                                      WHERE req.request_id=c.request_id
                                                        AND req.scan_profile=c.scan_profile
                                                        AND NOT c.duplicate
                                                        AND req.is_third_party) AS foo
                                                    GROUP BY foo.origin, foo.scan_profile
                                                    ORDER BY COUNT(*) DESC;""")

    profile_df = pd.DataFrame({'scan_profile': ['General', 'Red', 'Yellow', 'Blue', 'Green']})

    grouped_df = result_df_cookie_usage.groupby('profile')
    df1 = grouped_df.get_group(1) # normal
    df3 = grouped_df.get_group(3) # red
    df4 = grouped_df.get_group(4) # yellow
    df5 = grouped_df.get_group(5) # blue
    df6 = grouped_df.get_group(6) # green

    df_list = [df1, df3, df4, df5, df6]

    channel_1 = result_df_channels.loc[0][1]
    channel_3 = result_df_channels.loc[1][1]
    channel_4 = result_df_channels.loc[2][1]
    channel_5 = result_df_channels.loc[3][1]
    channel_6 = result_df_channels.loc[4][1]

    ch_list = [channel_1, channel_3, channel_4, channel_5, channel_6]

    data = list()

    for i in range(len(df_list)):
        df = df_list[i]

        col_count_list = df['count'].tolist()

        tp = len(df)
        max_value = df.loc[df['count'].idxmax()][2]
        min_value = df.loc[df['count'].idxmin()][2]
        sum = df.sum()[2]
        print(sum, ch_list[i])
        avg = round(ch_list[i]/sum, 2) # wie viele sind pro Schnitt im Channel
        sd = round(np.std(col_count_list),2)

        data.append({"3rd parties": tp, "AVG": avg , "MIN": min_value, "MAX": max_value, "SD": sd})

    df = pd.DataFrame.from_records(data)
    df.insert(0, 'scan_profile', ['General', 'Red', 'Yellow', 'Blue', 'Green'])
    #print(df.to_latex(index=False))
    print("USE SCRIPT IN BIQ QUERY!")


def general_cookie_statistics():
    """
    Computes and prints the latex code for the  cookie category overview table
    :return: None
    """
    # Compute cookie purpose across profiles
    result_df_cookie_purpose = exec_select_query(f"""SELECT
                                                  foo.scan_profile,
                                                  foo.purpose,
                                                  COUNT(*) AS total_number,
                                                  COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY scan_profile)*100 AS purpose_share
                                                FROM (
                                                  SELECT
                                                    DISTINCT scan_profile,
                                                    origin,
                                                    name,
                                                    purpose
                                                  FROM
                                                    `hbbtv-research.hbbtv.cookies`
                                                  WHERE
                                                    NOT duplicate) AS foo
                                                GROUP BY
                                                  scan_profile,
                                                  purpose;""")

    # Format all numbers and convert table into a pivot table.
    result_df_cookie_purpose['total_number'] = result_df_cookie_purpose['total_number'].apply('{:,}'.format)
    result_df_cookie_purpose['purpose_share'] = result_df_cookie_purpose['purpose_share'].apply('{:.2f}%'.format)
    pivot_cookie_purpose = result_df_cookie_purpose.pivot(index='scan_profile', columns='purpose')
    pivot_cookie_purpose['scan_profile1'] = pivot_cookie_purpose.index
    pivot_cookie_purpose.reset_index()

    # Sort table and include profile names
    pivot_cookie_purpose = pivot_cookie_purpose.sort_values(by=['scan_profile1'])
    pivot_cookie_purpose['scan_profile1'] = pivot_cookie_purpose['scan_profile1'].map({1: 'General', 3: 'Red',
                                                                                       4: 'Yellow', 5: 'Blue',
                                                                                       6: 'Green'})

    # Bring categories into the desired order
    pivot_cookie_purpose.columns = [' '.join(map(str, col)).strip() for col in pivot_cookie_purpose.columns.values]
    cols = ['scan_profile1', 'total_number Unknown', 'purpose_share Unknown', 'total_number Targeting/Advertising',
            'purpose_share Targeting/Advertising', 'total_number Performance', 'purpose_share Performance',
            'total_number Performance',  'purpose_share Performance', 'total_number Strictly Necessary',
            'purpose_share Strictly Necessary']
    pivot_cookie_purpose1 = pivot_cookie_purpose[cols]

    # Print latex code.
    # TODO For some reason the \midrule command is inserted after the first row not after the table header.
    print(pivot_cookie_purpose1.to_latex(index=False).replace('%', '\%'))


if __name__ == '__main__':
    #third_parties_cookie_usage()
    #general_cookie_statistics()
    plot_third_party_distribution()
    #cookie_overview(1)
    #cookie_usage_overview()

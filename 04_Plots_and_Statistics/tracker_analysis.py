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
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "resources", "google_bkp.json")
    #os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "..", "DatabasePusher", "resources", "google_bkp.json")
    client = bigquery.Client()

    # Execute the query and retrieve result data (as pandas dataframe)
    result_df = client.query(query).to_dataframe()

    return result_df


def get_fingerprinters():
    """
     Computes various statistics regarding fingerprinters that we observe in our measurement.

     :return: Nothing. Prints query results to standard out.
     """
    result_df = exec_select_query("""SELECT COUNT(DISTINCT req.channelname)
                                        FROM `hbbtv-research.hbbtv.responses` as res, `hbbtv-research.hbbtv.requests` as req
                                        WHERE res.request_id = req.request_id
                                        AND res.scan_profile = req.scan_profile
                                        AND res.type LIKE '%script%'
                                        AND (lower(res.response) LIKE '%webgl%' OR lower(res.response) LIKE '%fingerp%'OR lower(res.response) LIKE '%canvas%' OR lower(res.response) LIKE '%supercookie%')
                                        AND CAST(res.status AS int) = 200
                                        AND NOT req.is_iptv;""")

    print("[TR2.1] Number of channel on which fingerprinting was observed: ", result_df)

    result_df = exec_select_query("""SELECT COUNT( DISTINCT req.etld)
                                        FROM `hbbtv-research.hbbtv.responses` as res, `hbbtv-research.hbbtv.requests` as req
                                        WHERE res.request_id = req.request_id
                                        AND res.scan_profile = req.scan_profile
                                        AND res.type LIKE '%script%'
                                        AND (lower(res.response) LIKE '%webgl%' OR lower(res.response) LIKE '%fingerp%'OR lower(res.response) LIKE '%canvas%' OR lower(res.response) LIKE '%supercookie%')
                                        AND CAST(res.status AS int) = 200
                                        AND NOT req.is_iptv;""")

    print("[TR2.2] Number fingerprinters (trackers): ", result_df)

    result_df = exec_select_query("""SELECT COUNT(DISTINCT req.etld), req.is_first_party
                                        FROM `hbbtv-research.hbbtv.responses` as res, `hbbtv-research.hbbtv.requests` as req
                                        WHERE res.request_id = req.request_id
                                        AND res.scan_profile = req.scan_profile
                                        AND res.type LIKE '%script%'
                                        AND (lower(res.response) LIKE '%webgl%' OR lower(res.response) LIKE '%fingerp%'OR lower(res.response) LIKE '%canvas%' OR lower(res.response) LIKE '%supercookie%')
                                        AND CAST(res.status AS int) = 200
                                        AND NOT req.is_iptv
                                        GROUP BY req.is_first_party;""")

    print("[TR2.3] Number 1st-party and 3rd-party fingerprinters (trackers): ", result_df)

    result_df = exec_select_query("""SELECT COUNT(req.url), req.is_first_party
                                        FROM `hbbtv-research.hbbtv.responses` as res, `hbbtv-research.hbbtv.requests` as req
                                        WHERE res.request_id = req.request_id
                                        AND res.scan_profile = req.scan_profile
                                        AND res.type LIKE '%script%'
                                        AND (lower(res.response) LIKE '%webgl%' OR lower(res.response) LIKE '%fingerp%'OR lower(res.response) LIKE '%canvas%' OR lower(res.response) LIKE '%supercookie%')
                                        AND CAST(res.status AS int) = 200
                                        AND NOT req.is_iptv
                                        GROUP BY req.is_first_party;""")

    print("[TR2.4] Number 1st-party and 3rd-party fingerprinting requests: ", result_df)

    result_df = exec_select_query("""SELECT req.etld, COUNT(req.url)
                                        FROM `hbbtv-research.hbbtv.responses` as res, `hbbtv-research.hbbtv.requests` as req
                                        WHERE res.request_id = req.request_id
                                            AND res.scan_profile = req.scan_profile
                                            AND res.type LIKE '%script%'
                                            AND (lower(res.response) LIKE '%webgl%' OR lower(res.response) LIKE '%fingerp%'OR lower(res.response) LIKE '%canvas%' OR lower(res.response) LIKE '%supercookie%')
                                            AND CAST(res.status AS int) = 200
                                            AND NOT req.is_iptv
                                            AND req.is_known_tracker
                                        GROUP BY req.etld;""")

    print("[TR2.5] Number of flagged 1st-party and 3rd-party fingerprinting requests: ", result_df)

    # Is there a correlation by profile (scan_profile ~ fingerprinters)?
    result_df_profile_to_fingerprint = exec_select_query("""SELECT req.scan_profile, req.channelname, COUNT(*) AS fingerprinters
                                                        FROM `hbbtv-research.hbbtv.responses` as res, `hbbtv-research.hbbtv.requests` as req
                                                        WHERE res.request_id = req.request_id
                                                          AND res.scan_profile = req.scan_profile
                                                          AND res.type LIKE '%script%'
                                                          AND (lower(res.response) LIKE '%webgl%' OR lower(res.response) LIKE '%fingerp%'OR lower(res.response) LIKE '%canvas%' OR lower(res.response) LIKE '%supercookie%')
                                                          AND CAST(res.status AS int) = 200
                                                          AND NOT req.is_iptv
                                                        GROUP BY req.scan_profile, req.channelname;""")

    grouped_df = result_df_profile_to_fingerprint.groupby('scan_profile')
    df1 = grouped_df.get_group(1)['fingerprinters']  # normal
    df3 = grouped_df.get_group(3)['fingerprinters']  # red
    df4 = grouped_df.get_group(4)['fingerprinters']  # yellow
    df5 = grouped_df.get_group(5)['fingerprinters']  # blue
    df6 = grouped_df.get_group(6)['fingerprinters']  # green

    kruskal_profile_http = stats.kruskal(df1, df3, df4, df5, df6)
    n = len(result_df_profile_to_fingerprint)
    print("[TR2.6] Kruskal: scan profile ~ fingerprinting (p-value): ", kruskal_profile_http[1], "eta square:",
          compute_eta_square(kruskal_profile_http[0], 5, n))


def get_pixel_tracker():
    """
    Computes various statistics regarding tracking pixel  that we observe in our measurement.

    :return: Nothing. Prints query results to standard out.
    """

    # How many tracking pixel did we observe?
    result_df_pixel = exec_select_query("""SELECT COUNT(*)
                                        FROM `hbbtv-research.hbbtv.responses` as res, `hbbtv-research.hbbtv.requests` as req
                                        WHERE res.request_id = req.request_id
                                            AND res.scan_profile = req.scan_profile
                                            AND res.type LIKE '%image%'
                                            AND CAST(res.size AS int) < 45
                                            AND CAST(res.status AS int) = 200
                                            AND NOT req.is_iptv;""")
    total_trackers = result_df_pixel.iloc[0]
    print("[TR1.1] Potential tracking pixel: ", total_trackers)

    # How many tracking pixel did EasyList find?
    result_df = exec_select_query("""SELECT COUNT(*)
                                        FROM `hbbtv-research.hbbtv.responses` as res, `hbbtv-research.hbbtv.requests` as req
                                        WHERE res.request_id = req.request_id
                                            AND res.scan_profile = req.scan_profile
                                            AND res.type LIKE '%image%'
                                            AND CAST(res.size AS int) < 45
                                            AND CAST(res.status AS int) = 200
                                            AND req.is_known_tracker
                                            AND NOT req.is_iptv;""")
    print("[TR1.2] Known tracking pixel: ", result_df, "(", result_df.iloc[0] / total_trackers * 100, "%)")

    # How many trackers (etld+1) did we find?
    result_df = exec_select_query("""SELECT COUNT(DISTINCT req.etld)
                                        FROM `hbbtv-research.hbbtv.responses` as res, `hbbtv-research.hbbtv.requests` as req
                                        WHERE res.request_id = req.request_id
                                            AND res.scan_profile = req.scan_profile
                                            AND res.type LIKE '%image%'
                                            AND CAST(res.size AS int) < 45
                                            AND CAST(res.status AS int) = 200
                                            AND NOT req.is_iptv;""")
    number_trackers = result_df.iloc[0]
    print("[TR1.3] Number of trackers: ", number_trackers)

    # How many trackers (etld+1) did EasyList find?
    result_df = exec_select_query("""SELECT COUNT(DISTINCT req.etld)
                                        FROM `hbbtv-research.hbbtv.responses` as res, `hbbtv-research.hbbtv.requests` as req
                                        WHERE res.request_id = req.request_id
                                            AND res.scan_profile = req.scan_profile
                                            AND res.type LIKE '%image%'
                                            AND CAST(res.size AS int) < 45
                                            AND CAST(res.status AS int) = 200
                                            AND req.is_known_tracker
                                            AND NOT req.is_iptv;""")
    print("[TR1.4] Number of known trackers: ", result_df, "(", result_df.loc[0] / number_trackers * 100, "%)")

    # How many channels use a tracking pixel?
    result_df = exec_select_query("""SELECT COUNT(DISTINCT req.channelid)
                                        FROM `hbbtv-research.hbbtv.responses` as res, `hbbtv-research.hbbtv.requests` as req
                                        WHERE res.request_id = req.request_id
                                            AND res.scan_profile = req.scan_profile
                                            AND res.type LIKE '%image%'
                                            AND CAST(res.size AS int) < 45
                                            AND CAST(res.status AS int) = 200
                                            AND NOT req.is_iptv;""")
    print("[TR1.5] Channels using a tracking pixel: ", result_df)

    # Is there a correlation by profile (scan_profile ~ tracking pixel)?
    result_df_profile_to_pixel = exec_select_query("""SELECT req.scan_profile, channelname, COUNT(*) AS tracking_pixel
                                                        FROM `hbbtv-research.hbbtv.responses` as res, `hbbtv-research.hbbtv.requests` as req
                                                        WHERE res.request_id = req.request_id
                                                            AND res.scan_profile = req.scan_profile
                                                            AND res.type LIKE '%image%'
                                                            AND CAST(res.size AS int) < 45
                                                            AND CAST(res.status AS int) = 200
                                                            AND NOT req.is_iptv
                                                        GROUP BY req.scan_profile, req.channelname;""")

    grouped_df = result_df_profile_to_pixel.groupby('scan_profile')
    df1 = grouped_df.get_group(1)['tracking_pixel']  # normal
    df3 = grouped_df.get_group(3)['tracking_pixel']  # red
    df4 = grouped_df.get_group(4)['tracking_pixel']  # yellow
    df5 = grouped_df.get_group(5)['tracking_pixel']  # blue
    df6 = grouped_df.get_group(6)['tracking_pixel']  # green

    kruskal_profile_http = stats.kruskal(df1, df3, df4, df5, df6)
    n = len(result_df_profile_to_pixel)
    print("[TR1.6] Kruskal: scan profile ~ tracking pixel (p-value): ", kruskal_profile_http[1], "eta square:",
          compute_eta_square(kruskal_profile_http[0], 5, n))

    # Share of tracking pixel traffic (compared to overall traffic)
    result_df_tracking_pixel_share = exec_select_query("""SELECT
                                                          (
                                                          SELECT
                                                            COUNT (*) AS tracking_pixel
                                                          FROM
                                                            `hbbtv-research.hbbtv.responses` AS res,
                                                            `hbbtv-research.hbbtv.requests` AS req
                                                          WHERE
                                                            res.request_id = req.request_id
                                                            AND res.scan_profile = req.scan_profile
                                                            AND res.type LIKE '%image%'
                                                            AND CAST(res.size AS int) < 45
                                                            AND CAST(res.status AS int) = 200
                                                            AND NOT req.is_iptv) / COUNT(*) * 100.0
                                                        FROM
                                                          `hbbtv-research.hbbtv.requests`;""")

    print("[TR1.7] Share of tracking pixel traffic (compared to overall traffic):", result_df_tracking_pixel_share)

    # Number and share of channels using "tvping.com" as a tracker.
    result_df_tvping_share = exec_select_query("""WITH
                                                  channel_tvping AS (
                                                  SELECT
                                                    DISTINCT foo.channelname,
                                                    COUNT(*) AS count
                                                  FROM (
                                                    SELECT
                                                      DISTINCT req.etld,
                                                      res.size,
                                                      req.channelname,
                                                      COUNT(*) AS tracking_pixel
                                                    FROM
                                                      `hbbtv-research.hbbtv.responses` AS res,
                                                      `hbbtv-research.hbbtv.requests` AS req
                                                    WHERE
                                                      res.request_id = req.request_id
                                                      AND res.scan_profile = req.scan_profile
                                                      AND res.type LIKE '%image%'
                                                      AND CAST(res.size AS int) < 45
                                                      AND CAST(res.status AS int) = 200
                                                      AND NOT req.is_iptv
                                                    GROUP BY
                                                      req.etld,
                                                      req.channelname,
                                                      res.size
                                                    ORDER BY
                                                      tracking_pixel DESC) AS foo
                                                  GROUP BY
                                                    foo.channelname)
                                                SELECT
                                                  COUNT (DISTINCT req.channelname) AS channel_count,
                                                  COUNT (DISTINCT req.channelname) / (
                                                  SELECT
                                                    COUNT(DISTINCT programId)
                                                  FROM
                                                    `hbbtv-research.hbbtv.channel_details`) * 100 AS channel_share
                                                FROM
                                                  `hbbtv-research.hbbtv.requests` req,
                                                  channel_tvping ctv
                                                WHERE
                                                  req.channelname!=ctv.channelname;""")

    print("[TR1.8] Number and share of channels using 'tvping.com' as a tracker.):", result_df_tvping_share)


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


def get_tracking_overview():

    # Get all blocked requests
    result_df_easylist_total = exec_select_query("""SELECT COUNT(*) AS easylist_blocked
                                                FROM `hbbtv-research.hbbtv.requests`
                                                WHERE is_known_tracker;""")
    print("[TR0.1] Blocked by easylist: ", result_df_easylist_total)

    result_df_pihole_total = exec_select_query("""SELECT COUNT(*) AS pihole_blocked
                                                    FROM `hbbtv-research.hbbtv.requests`
                                                    WHERE pi_hole_blocked;""")
    print("[TR0.2] Blocked by Pi-hole: ", result_df_pihole_total)

    # Get all blocked requests
    result_df_easylist_privacy_total = exec_select_query("""SELECT COUNT(*) AS easylist_blocked
                                                FROM `hbbtv-research.hbbtv.requests`
                                                WHERE is_easylist_privacy_blocked;""")
    print("[TR0.3] Blocked by easylist privacy: ", result_df_easylist_privacy_total)

    # How many request were flagged by EasyList?
    result_df_easylist = exec_select_query("""SELECT scan_profile, COUNT(*) AS easylist_blocked
                                                FROM `hbbtv-research.hbbtv.requests`
                                                WHERE is_known_tracker
                                                GROUP BY scan_profile;""")

    # How many request were flagged by PiHole?
    result_df_pihole = exec_select_query("""SELECT scan_profile, COUNT(*) AS pihole_blocked
                                            FROM `hbbtv-research.hbbtv.requests`
                                            WHERE pi_hole_blocked
                                            GROUP BY scan_profile;""")
    merged_df = pd.merge(result_df_pihole, result_df_easylist, on='scan_profile')
    merged_df['easylist_blocked'] = merged_df['easylist_blocked'].map('{:,}'.format)
    merged_df['pihole_blocked'] = merged_df['pihole_blocked'].map('{:,}'.format)

    # How many tracking pixel did we observe?
    result_df_tracking_pixel = exec_select_query("""SELECT req.scan_profile, COUNT(*) AS tracking_pixel
                                                    FROM `hbbtv-research.hbbtv.responses` as res, `hbbtv-research.hbbtv.requests` as req
                                                    WHERE res.request_id = req.request_id
                                                       AND res.scan_profile = req.scan_profile
                                                       AND res.type LIKE '%image%'
                                                       AND CAST(res.size AS int) < 45
                                                       AND CAST(res.status AS int) = 200
                                                       AND NOT req.is_iptv
                                                    GROUP BY req.scan_profile;""")
    merged_df = pd.merge(merged_df, result_df_tracking_pixel, on='scan_profile', how='outer')
    merged_df['tracking_pixel'] = merged_df['tracking_pixel'].map('{:,}'.format)

    # How many fingerprinting requests did we observe?
    result_df_fingerprinting = exec_select_query("""SELECT req.scan_profile, COUNT(*) AS fingerprinting
                                                    FROM `hbbtv-research.hbbtv.responses` as res, `hbbtv-research.hbbtv.requests` as req
                                                    WHERE res.request_id = req.request_id
                                                      AND res.scan_profile = req.scan_profile
                                                      AND res.type LIKE '%script%'
                                                      AND (lower(res.response) LIKE '%webgl%' OR lower(res.response) LIKE '%fingerp%'OR lower(res.response) LIKE '%canvas%' OR lower(res.response) LIKE '%supercookie%')
                                                      AND CAST(res.status AS int) = 200
                                                      AND NOT req.is_iptv
                                                    GROUP BY req.scan_profile;""")
    merged_df = pd.merge(merged_df, result_df_fingerprinting, on='scan_profile', how='outer')
    merged_df['fingerprinting'] = merged_df['fingerprinting'].map('{:,}'.format)

    # Sort and include profile names
    merged_df = merged_df.sort_values(by=['scan_profile'])
    merged_df['scan_profile'] = merged_df['scan_profile'].map({1: 'General', 3: 'Red', 4: 'Yellow', 5: 'Blue', 6: 'Green'})

    # Print table
    print(merged_df.to_latex(index=False, na_rep='0').replace('<NA>', '0'))


def get_children_tracking():
    # Number of channels that provide program for children
    result_df = exec_select_query("""SELECT COUNT(*)
                                        FROM `hbbtv-research.hbbtv.channellist`
                                        WHERE channel_category = '{\\'Children\\'}';""")
    print("[TR3.1] Number of channels in the children category:", result_df)

    # Number of trackers on "children" channels
    result_df = exec_select_query("""SELECT  COUNT(*)
                                        FROM `hbbtv-research.hbbtv.channellist` AS chan, `hbbtv-research.hbbtv_backup.requests` AS req,`hbbtv-research.hbbtv.responses` AS res
                                        WHERE chan.channel_category = '{\\'Children\\'}'
                                          AND req.channelid = chan.programId
                                          AND ( (req.is_known_tracker)
                                                OR (res.type LIKE '%image%'
                                                    AND CAST(res.size AS int) < 45
                                                    AND CAST(res.status AS int) = 200)
                                                OR ((lower(res.response) LIKE '%webgl%' OR lower(res.response) LIKE '%fingerp%'OR lower(res.response) LIKE '%canvas%' OR lower(res.response) LIKE '%supercookie%'))
                                              )
                                          AND res.request_id = req.request_id
                                          AND res.scan_profile = req.scan_profile
                                          AND NOT req.is_iptv;""")

    print("[TR3.2] Number of trackers on 'children' channels:", result_df)

    # Number of third party cookies on children's TV
    result_df_child_cookies = exec_select_query("""SELECT
                                                      COUNT(*)
                                                    FROM (
                                                      SELECT
                                                        DISTINCT coo.name,
                                                        coo.origin,
                                                        coo.path
                                                    FROM
                                                      `hbbtv-research.hbbtv.channellist` AS chan,
                                                      `hbbtv-research.hbbtv.cookies` AS coo,
                                                      `hbbtv-research.hbbtv.requests` AS req
                                                    WHERE
                                                      channel_category = '{\\'Children\\'}'
                                                      AND NOT coo.duplicate
                                                      AND req.is_third_party
                                                      AND coo.request_id = req.request_id
                                                      AND coo.scan_profile = req.scan_profile
                                                      AND NOT req.is_iptv
                                                      AND coo.purpose = 'Targeting/Advertising');""")

    print("[TR3.3] Number of third party cookies on children's TV: ", result_df_child_cookies)

    # Is there a correlation by channel category (channel_cat ~ num_trackers)?
    result_df_cat_to_tracker = exec_select_query("""SELECT channel_category, req.channelid, COUNT(*) AS trackers
                                                            FROM
                                                              `hbbtv-research.hbbtv.channellist` AS chan,
                                                              `hbbtv-research.hbbtv_backup.requests` AS req,
                                                              `hbbtv-research.hbbtv.responses` AS res
                                                            WHERE
                                                              req.channelid = chan.programId
                                                              AND ( (req.is_known_tracker)
                                                                OR (res.type LIKE '%image%'
                                                                  AND CAST(res.size AS int) < 45
                                                                  AND CAST(res.status AS int) = 200)
                                                                OR ((LOWER(res.response) LIKE '%webgl%'
                                                                    OR LOWER(res.response) LIKE '%fingerp%'OR LOWER(res.response) LIKE '%canvas%'
                                                                    OR LOWER(res.response) LIKE '%supercookie%')) )
                                                              AND res.request_id = req.request_id
                                                              AND res.scan_profile = req.scan_profile
                                                              AND NOT req.is_iptv
                                                            GROUP BY chan.channel_category, req.channelid;""")

    # Get values for the Wilcoxon-Mann-Whitney-Test
    grouped_df = result_df_cat_to_tracker.groupby('channel_category')
    all_groups = grouped_df.groups.keys()
    comp_dfs = []
    for group in all_groups:
        if group != "{'Children'}":
            comp_dfs.extend(grouped_df.get_group(group)['trackers'])
    df_children = grouped_df.get_group("{'Children'}")['trackers'].to_list()

    # Compute Wilcoxon-Mann-Whitney-Test
    wilcoxon_res = stats.mannwhitneyu(df_children, comp_dfs)
    print("[TR3.4] Wilcoxon-Mann-Whitney-Test: channel_cat profile ~ num_trackers: ", wilcoxon_res)


if __name__ == '__main__':
    # Analyze observed trackers (e.g., tracking pixel or TV fingerprinting).
    # get_tracking_overview()
    # get_pixel_tracker()
    # get_fingerprinters()
    get_children_tracking()

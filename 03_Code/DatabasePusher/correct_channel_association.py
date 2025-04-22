import json
from google.cloud import bigquery
import os
import ast
import sys
import datetime


def exec_select_query(query):
    """
    Executes the given SQL query using the static Google authentication credentials.

    :param query: The SQL query
    :return: A (pandas) dataframe that contains the results
    """
    # Initialize teh Google BigQuery client. The authentication token should be placed in the working directory in the
    # following path: /resources/google.json
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "resources", "google_bkp.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "..", "DatabasePusher", "resources",
                                                                "google_bkp.json")
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


def update_new_channel(request_id, scan_profile, new_channel_name, new_channel_id, update=True):
    """
    Updates the header fields in the request and response table.

    :param request_id: The request ID of the request to update.
    :param scan_profile: The scan profile of the request to update.
    :param new_channel_name: The new channel name.
    :param new_channel_id: The new channel ID.
    :param update: Simple flag for debugging reason to indicate if we found a new channel.
    :return: None
    """

    # For debugging reasons
    if not update:
        # Update requests table
        # We perform a sanity check so that only one row will be affected.
        # TODO Should be removed once we are confident in our data... Because it is WAAAAAY too slow
        update_count = exec_select_query(f"""SELECT COUNT(*) AS count
                                            FROM `hbbtv.requests`
                                            WHERE request_id={request_id}
                                                AND scan_profile={scan_profile};""")

        affected_rows = update_count.iloc[:1]['count'][0]
        if affected_rows != 1:
            print("I would have updated %d rows! Aborting. Debug: request=%d profile=%d channelname=%s channelid=%s" % (
                affected_rows, request_id, scan_profile, new_channel_name, new_channel_id))
            return
        else:
            print("Updating! Debug: request=%d profile=%d channelname=%s channelid=%s" % (request_id, scan_profile, new_channel_name, new_channel_id))

    else:
        update_requests_query = f""" UPDATE `hbbtv.requests`
                               SET correct_channelname=r'{new_channel_name}', correct_channelid=r'{new_channel_id}'
                               WHERE request_id={request_id}
                                AND scan_profile={scan_profile};"""
        # TODO uncomment if we are confident...
        #exec_update_query(update_requests_query)

        # Update response table
        update_response_query = f""" UPDATE `hbbtv.responses`
                               SET correct_channelname=r'{new_channel_name}', correct_channelid=r'{new_channel_id}'
                               WHERE request_id={request_id}
                                AND scan_profile={scan_profile};"""
        # TODO uncomment if we are confident...
        #exec_update_query(update_response_query)


def analyze_requests(profile):
    # Get all requests
    all_responses = exec_select_query(f"""SELECT * FROM `hbbtv.requests` WHERE scan_profile={profile};""")

    # Iterate over request headers
    for index, resp in all_responses.iterrows():
        # Reformat headers
        json_object = ast.literal_eval(resp["headers"])
        header_dict = {item['name']: item for item in json_object}

        # Get referer header
        referer_header = header_dict.get("Referer", None)
        if referer_header is not None:
            referer = referer_header['value']
            if referer != resp['url']:
                # Find the issuing request
                get_issuing_request(referer, resp['request_id'], resp['scan_profile'], resp['time_stamp'],
                                    resp['channelname'], resp['channelid'])
        else:
            # We do not need to adjust the channel. Yet, we have to set the "new channel" field using the current
            # channel.
            update_new_channel(resp['request_id'], resp['scan_profile'], resp['channelname'], resp['channelid'])


sanity_problems = {'5-6': 0, '6-10':0, '10-15':0, '15<':0}


def get_issuing_request(referer, request_id, scan_profile, timestamp, channel_name, channel_id):
    # We only want to consider requests that are at most 5 minutes old.
    sanity_check_minutes = 15
    sanity_timestamp = timestamp - datetime.timedelta(hours=0, minutes=sanity_check_minutes)

    # Get all suiting candidates
    query = """SELECT *
                FROM `hbbtv.requests`
                WHERE url = r'%s'
                    AND request_id < %d
                    AND scan_profile = %d
                    AND time_stamp >= '%s'
                ORDER BY request_id ASC;""" % (referer, request_id, scan_profile, sanity_timestamp)
    referring_candidates = exec_select_query(query)

    # There may be 0, 1, or >1 result candidates.
    if len(referring_candidates) == 0:
        # If we cannot find a suiting request within our sanity time frame, we do not update the channel.
        # update_new_channel(request_id, scan_profile, channel_name, channel_id, update=False)
        print("THIS SHOULD ONLY RARELY HAPPEN: r=%s, id=%s, sp=%s" % (referer, request_id, scan_profile))
        #
        # #print("Check without sanity check...")
        # # Get all suiting candidates
        query = """SELECT *
                    FROM `hbbtv.requests`
                    WHERE url = r'%s'
                        AND request_id < %d
                        AND scan_profile = %d
                    ORDER BY request_id ASC;""" % (referer, request_id, scan_profile)
        ref_can = exec_select_query(query)
        if len(ref_can):
            diff_from_sanity_check = timestamp - ref_can['time_stamp'].tolist()[0]
            if diff_from_sanity_check < datetime.timedelta(minutes=6):
                sanity_problems['5-6'] += 1
            elif diff_from_sanity_check < datetime.timedelta(minutes=10) and diff_from_sanity_check > datetime.timedelta(minutes=6):
                sanity_problems['6-10'] += 1
            elif diff_from_sanity_check < datetime.timedelta(minutes=15) and diff_from_sanity_check > datetime.timedelta(minutes=10):
                sanity_problems['10-15'] += 1
            elif diff_from_sanity_check > datetime.timedelta(minutes=15):
                sanity_problems['15<'] += 1
        # else:
        #     # Get only one candidate
        #     result_row = ref_can.iloc[-1]
        #     diff_from_sanity_check = timestamp - ref_can['time_stamp'].tolist()[0]
        #     if diff_from_sanity_check < datetime.timedelta(minutes=6):
        #         sanity_problems['5-6'] += 1
        #     elif diff_from_sanity_check < datetime.timedelta(minutes=10) and diff_from_sanity_check > datetime.timedelta(minutes=6):
        #         sanity_problems['6-10'] += 1
        #     elif diff_from_sanity_check < datetime.timedelta(minutes=15) and diff_from_sanity_check > datetime.timedelta(minutes=10):
        #         sanity_problems['10-15'] += 1
        #     elif diff_from_sanity_check > datetime.timedelta(minutes=15):
        #         sanity_problems['15<'] += 1

        # print("SELECT * FROM `hbbtv.requests` WHERE request_id = %d AND scan_profile=%d" % (request_id, scan_profile))

    elif len(referring_candidates) == 1:
        # We found one candidate, and we use it.
        result_row = referring_candidates.iloc[:1]
        new_channel_id = result_row['channelid'][0]
        new_channel_name = result_row['channelname'][0]
        update_new_channel(request_id, scan_profile, new_channel_name, new_channel_id, update=True)

    elif len(referring_candidates) > 1:
        # We found multiple requests and only use one with the highest request_id (i.e, most recent request
        # matching our definition). Good thing that we order by the results by the request ID.
        result_row = referring_candidates.iloc[-1]
        new_channel_id = result_row['channelid'][0]
        new_channel_name = result_row['channelname'][0]
        update_new_channel(request_id, scan_profile, new_channel_name, new_channel_id, update=True)


if __name__ == '__main__':
    analyze_requests(1)

    for k, v in sanity_problems.items():
        print(k, v)

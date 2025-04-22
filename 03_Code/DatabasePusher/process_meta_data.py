import time
import datetime
import os
import logging
import json
import sys
import ast
from push_ops import stream_to_BQ

formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(funcName)-30s %(message)s')
# The log file is the same as the module name plus the suffix ".log"
# i.e.: calculate.py -> calculate.py.log
fh = logging.FileHandler("%s.log" % (os.path.join("logs", os.path.basename(__file__))))
sh = logging.StreamHandler()
fh.setLevel(logging.DEBUG)      # set the log level for the log file
fh.setFormatter(formatter)
sh.setFormatter(formatter)
sh.setLevel(logging.INFO)       # set the log level for the console
logger = logging.getLogger(__name__)
logger.addHandler(fh)
logger.addHandler(sh)
logger.setLevel(logging.INFO)
logger.propagate = False


def write_meta_data(measurement_id):

    path_log = os.path.join(os.getcwd(), "00_Data", "meta_data")
    log_files = os.listdir(path_log)
    logger.info(f"Found {len(log_files)} log files")

    # Iterate over all log files and push teh entries into the BQ database
    for file in log_files:
        logger.info(f"Read file {file}")

        # Get the profile ID
        # measurement_id = file.split('_')[0]
        #TODO!!!!!!
        # measurement_id = 1
        # Extract log entries from the log file
        log_entries = get_log_objects(os.path.join(path_log, file), measurement_id)

        # Push data to BigQuery
        stream_to_BQ('hbbtv-research.hbbtv.LogData', log_entries)


def get_log_objects(log_file, measurement_id):
    jsonObj = []

    # Read the log file
    with open(log_file, 'r') as f:
        lines = f.readlines()
        logger.info(f"Read {len(lines)} lines out of file {log_file}")
        for line in lines:
            # Adjust the malformed JSON objects (remove ", " at the end).
            # line = line.strip()[:-1]
            jsonObj.append(json.loads(line))

    logs = []
    # Fill to BigQuery data object.
    for entry in jsonObj:
        metadata = dict()
        metadata['scan_profile'] = measurement_id
        metadata['command'] = entry['command']
        metadata['current_channel'] = entry['channel_before']
        metadata['channel_after'] = entry['channel_after']
        metadata['result'] = entry['result']
        t = time.mktime(datetime.datetime.strptime(entry['time_stamp'], "%a %b %d %H:%M:%S %Y").timetuple())
        metadata['time_stamp'] = datetime.datetime.fromtimestamp(t).isoformat()


        # # TODO wieso kommt das vor??
        # # TODO need proper review and maybe a redesign
        # if len(entry['channel_before']) > 0:
        #     tmp = ast.literal_eval(entry['channel_before'])
        #     if type(tmp) == int:
        #         metadata['current_channel'] = tmp
        #     else:
        #         metadata['current_channel'] = tmp['channelId']
        # else:
        #     metadata['current_channel'] = "Unknown"
        # # TODO wieso kommt das vor??
        # if len(entry['channel_after']) > 0:
        #     tmp = ast.literal_eval(entry['channel_after'])
        #     if type(tmp) == int:
        #         metadata['new_channel'] = tmp
        #     else:
        #         metadata['new_channel'] = tmp['channelId']
        # else:
        #     metadata['new_channel'] = "Unknown"
        # # metadata['new_channel'] = entry['channel_after']#['channelId']

        # t = time.mktime(datetime.datetime.strptime(entry['time_stamp'], "%a %b %d %H:%M:%S %Y").timetuple())
        # metadata['time_stamp'] = datetime.datetime.fromtimestamp(t).isoformat()
        #
        # # print(metadata['time_stamp'])
        # metadata['scan_profile'] = entry['profile']
        # # data = entry['data']
        # metadata['result'] = str(entry['result'])

        # TODO TU: Ich verstehe den Sinn des codes nicht. Der Command wird mit dem Ergebnis des Commands überschrieben?
        # if command == "get_current_channel_program_info":
        #     metadata['command'] = data
        # if command == "get_channel_data":
        #     metadata['command'] = data
        # if command == "get_program_data":
        #     metadata['command'] = data
        # if command == "get_channel_id":
        #     metadata['command'] = data
        # if command == "get_current_program":
        #     metadata['command'] = data

        # if (command == interaction for interaction in interaction_set):
        #     metadata['interaction_command'] = data

        # metadata['profile'] = entry['profile']
        logger.debug(f"Read log entry: {metadata}")
        logs.append(metadata)

    return logs

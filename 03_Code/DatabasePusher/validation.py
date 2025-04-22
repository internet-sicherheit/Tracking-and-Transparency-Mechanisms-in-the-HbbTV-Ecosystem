#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Provides fundamental instructions to control the hbbtv and collect data from
channels.

Methods based on the PyWebOSTV from

https://github.com/supersaiyanmode/PyWebOSTV

"""
__author__ = "Christian M. Boettger, Moritz Kappels, Nurullah Demir"

__license__ = ""
__version__ = ""
__maintainer__ = ""
__status__ = "Prototype"

# Imports
import os, logging
import glob

from haralyzer import HarParser

# Initialize logging
# Each log line includes the date and time, the log level, the current function and the message
formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(funcName)-30s %(message)s')
# The log file is the same as the module name plus the suffix ".log"
# i.e.: calculate.py -> calculate.py.log
fh = logging.FileHandler("%s.log" % (os.path.basename(__file__)))
sh = logging.StreamHandler()
fh.setLevel(logging.DEBUG)      # set the log level for the log file
fh.setFormatter(formatter)
sh.setFormatter(formatter)
sh.setLevel(logging.INFO)       # set the log level for the console
logger = logging.getLogger(__name__)
logger.addHandler(fh)
logger.addHandler(sh)
logger.setLevel(logging.DEBUG)
logger.propagate = False

path_har_files = os.getcwd() + '/doc/measurement_data/'

class Validation:
    def __init__(self, path):
        self.path = path
        self.files = glob.glob(self.path+"*.har")
        logger.info(f"Found {len(self.files)} har files!")

    def analyse(self):
        requests = []
        responses = []
        cookies = []

        counter_no_channel_selected = 0
        counter_channel_selected = 0
        total_channel = 0

        for file in self.files:
            logger.info(f"Read file {file}")
            har_data = self.process_har(file)

            i = 0
            for entry in har_data['entries']:
                logger.info(f"ChannelName: {entry['request']['channelName']}")
                i = i + 1
                total_channel +=1
                # Request
                if entry['request']['channelName'] != 'No channel selected':
                    counter_channel_selected += 1
                    req = {}
                    req['request_id'] = i
                    req['channelName'] = entry['request']['channelName']

                    req['channelID'] = entry['request']['channelID']
                    req['url'] = entry['request']['url']
                    req['method'] = entry['request']['method']
                    #print(req)
                    requests.append(req)
                else:
                    counter_no_channel_selected += 1

                # Response
                if entry['request']['channelName'] != 'No channel selected':
                    counter_channel_selected += 1
                    resp = {}
                    resp['request_id'] = i
                    resp['channelName'] = entry['request']['channelName']
                    resp['channelID'] = entry['request']['channelID']
                    resp['url'] = entry['request']['url']
                    #print(resp)
                    responses.append(resp)
                else:
                    counter_no_channel_selected += 1
        else:
            print(f"Total channel: {total_channel}")
            print(f"No channel: {counter_no_channel_selected/2}")
            print(f"Channel: {counter_channel_selected}")

    def process_har(self, file):
        har_parser = HarParser.from_file(file)
        har_data = har_parser.har_data
        har_data['entries'] = [entry for entry in har_data['entries']]

        return har_data


if __name__ == '__main__':
    v = Validation(path_har_files)
    v.analyse()

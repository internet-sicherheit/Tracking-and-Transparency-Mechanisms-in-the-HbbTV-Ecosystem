#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Provides fundamental instructions to control the hbbtv and collect data from
channels.

Methods based on the PyWebOSTV from

https://github.com/supersaiyanmode/PyWebOSTV

"""

import time
import random
import urllib.request
import datetime
import unicodedata
import re
import glob
#from datetime import datetime
import pywebostv.controls
from pywebostv.connection import *
# from pywebostv.controls import * <-- don't import all classes
# instead import them individually
from pywebostv.controls import WebOSControlBase, SystemControl, InputControl, MediaControl
from pywebostv.discovery import *

import json
from ping3 import ping, verbose_ping
import os
import random
import subprocess
from Fallback import FallbackLogger
import pandas as pd

# imports from project folder

#import data_processing as phar


# Initialize logging
import os, logging
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

dt = datetime.datetime.now()
ts = datetime.datetime.timestamp(dt)

# Output file for the session
global basepath, struct_meta, struct_screenshots
global output_file
global screenshot_folder

def createDataStructure(restore=False):
    """
    Generate Folder for measurement
    """
    global basepath, struct_meta, struct_screenshots
    global output_file
    global screenshot_folder

    b_path = os.path.join(os.getcwd(), '..', '..' , '02_Measurement_Data', 'measurements')
    if restore:
        num = 0
        folders = os.listdir(b_path)
        last_folder_path = ""
        if len(folders) > 0:

            for folder in folders:
                name = int(folder.split("_")[0])
                if name > num:
                    num = name
                    last_folder_path = folder

        path = last_folder_path
        meta_data = last_folder_path + "meta_data/"
        screenshot_folder = last_folder_path + "captures/"
        har = last_folder_path + "hardump/"

        return path, meta_data, screenshot_folder

    else:
        logger.info("Create folder strcuture")
        num = 0
        folders = os.listdir(b_path)
        if len(folders) > 0:

            for folder in folders:
                name = int(folder.split("_")[0])
                if name > num:
                    num = name
            else:
                num += 1
        else:
            num = 1

        path = b_path+str(num)+"_Measurement_"+str(datetime.datetime.now().strftime("%Y-%m-%d"))+"/"
        meta_data = path + "meta_data/"
        screenshot_folder = path + "captures/"
        har = path + "hardump/"
        tv_export = path + "tv_export/"

        logger.info(f"Create basepath on {path}")
        os.mkdir(path)
        os.mkdir(meta_data)
        os.mkdir(screenshot_folder)
        os.mkdir(har)
        os.mkdir(tv_export)

        with open("{path}unfug2.txt", "w") as f:
            f.write(har)


        basepath, struct_meta, struct_screenshots = path, meta_data, screenshot_folder
        output_file = struct_meta + 'Metadata_' + str(datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")) + '.txt'
        screenshot_folder = struct_screenshots

        logger.info("Successfully created folder structure")
        return path, meta_data, screenshot_folder





# ---------------------------------------------------------------
# monkey patched version of 'class TvControl(WebOSControlBase):'
# this is needed for "screenshot"
# ---------------------------------------------------------------

class TvControl(pywebostv.controls.TvControl):

    COMMANDS = {
        "channel_down": {"uri": "ssap://tv/channelDown"},
        "channel_up": {"uri": "ssap://tv/channelUp"},
        "set_channel_with_id": {
            "uri": "ssap://tv/openChannel",
            "args": [str],
            "payload": {
                "channelId":  pywebostv.controls.arguments(0)
            }
        },
        "get_current_channel": {
            "uri": "ssap://tv/getCurrentChannel",
            "validation":  pywebostv.controls.standard_validation,
            "subscription_validation": pywebostv.controls.subscription_validation,
            "subscription": True
        },

        "channel_list": {"uri": "ssap://tv/getChannelList"},
        "get_current_program": {
            "uri": "ssap://tv/getChannelProgramInfo",
            "validation":  pywebostv.controls.standard_validation
        },
        "get_current_channel_program_info": {
            "uri": "ssap://tv/getChannelCurrentProgramInfo",
            "validation": pywebostv.controls.standard_validation
        },

        "execute_Oneshot": {
            "uri": "ssap://tv/executeOneShot",
            "validation":  pywebostv.controls.standard_validation,
            "path": ".\capture.jpg",
            "method": "DISPLAY",
            "format": "JPG"
        }
    }



# set local TvControl as source every time TvControl from library is called
pywebostv.controls.TvControl = TvControl

# ---------------------------------------------------------------
#  _____ _   _ _____ _____
# |_   _| \ | |_   _|_   _|
#   | | |  \| | | |   | |
#   | | | . ` | | |   | |
#  _| |_| |\  |_| |_  | |
#  \___/\_| \_/\___/  \_/
#
#
#  _____ _____ ___  ______ _____
# /  ___|_   _/ _ \ | ___ \_   _|
# \ `--.  | |/ /_\ \| |_/ / | |
#  `--. \ | ||  _  ||    /  | |
# /\__/ / | || | | || |\ \  | |
# \____/  \_/\_| |_/\_| \_| \_/
#

# this method establishes the connection and
# intitiates all apis + checks if they work
tv_initiated = False


def tv_init():
    logger.info("Init lg tv setup")
    # this key appeared during registration:
    store = {'client_key': '{Client Key}'}
    logger.info(f"Registrate on tv with key: {store}")
    # check ip address in options-menu of TV
    # also ping the ip address to make sure it is reachable
    global ip_tv
    ip_tv = "{IP of TV}"
    logger.info(f"Connect to hbbtv with ip {ip_tv}")
    client = WebOSClient(ip_tv)
    client.connect()
    # check if remote device is allready registered
    for status in client.register(store):
        if status == WebOSClient.PROMPTED:
            logger.info("User interaction required")
            print("Please accept the connect on the TV!")
        elif status == WebOSClient.REGISTERED:
            logger.info("Registration successful!")

    # create dictionaries where all logs are temporary stored
    global interact_logs
    interact_logs = []

    # init global variables for WebOS controls
    global tv_media  # volume up/down, mute/unmute, stop, rewind etc
    tv_media = MediaControl(client)
    global tv_control  # channeldata and screenshots (catch response)
    tv_control = TvControl(client)
    global tv_system  # power off, screen off, info
    tv_system = SystemControl(client)
    global tv_input  # controls all key inputs (= digital remote control)
    tv_input = InputControl(client)
    tv_input.connect_input()  # + tv_input.disconnect_input()


    # check if apis where properly inititated
    media_c = str(
        type(tv_media)) == "<class 'pywebostv.controls.MediaControl'>"
    control_c = str(type(tv_control)) == "<class '__main__.TvControl'>" or str(type(tv_control)) == "<class 'remote_tv.TvControl'>"
    system_c = str(
        type(tv_system)) == "<class 'pywebostv.controls.SystemControl'>"
    input_c = str(
        type(tv_input)) == "<class 'pywebostv.controls.InputControl'>"

    global tv_initiated
    tv_initiated = media_c and system_c and input_c and control_c

    if (tv_initiated):
        logger.info("PyWebOSTV Controls where properly initiated.")
    else:
        logger.error("Error: PyWebOSTV Controls where NOT properly initiated (check init section).")
        logger.error(f"{media_c}, {system_c}, {input_c}, {control_c}")

    return tv_initiated

#  _____ _   _ _____ _____
# |_   _| \ | |_   _|_   _|
#   | | |  \| | | |   | |
#   | | | . ` | | |   | |
#  _| |_| |\  |_| |_  | |
#  \___/\_| \_/\___/  \_/
#
#
#  _____ _   _______
# |  ___| \ | |  _  \
# | |__ |  \| | | | |
# |  __|| . ` | | | |
# | |___| |\  | |/ /
# \____/\_| \_/___/
#
# ---------------------------------------------------------------
#
# ---------------------------------------------------------------
#  _     _____ _____
# | |   |  _  |  __ \
# | |   | | | | |  \/
# | |   | | | | | __
# | |___\ \_/ / |_\ \
# \_____/\___/ \____/
#
#
#  _____ _____ ___  ______ _____
# /  ___|_   _/ _ \ | ___ \_   _|
# \ `--.  | |/ /_\ \| |_/ / | |
#  `--. \ | ||  _  ||    /  | |
# /\__/ / | || | | || |\ \  | |
# \____/  \_/\_| |_/\_| \_| \_/
#


# log all activity
"""
def createLog(
    channel_before,  # channel info before interaction
    program_before,  # program info before interaction
    command,  # command that is being executed
    channel_after,  # channel info after interaction
    program_after,  # program info after interaction
    data, # data behind the command
    profile # current watchprofile
):
    logger.info("Create log for channel information")

    time_stamp = time.time()  # current time stamp
    readable_time = time.ctime(int(time_stamp))  # time in readable format
    logs = {}  # the dictionary for the logs of a single interaction
    logs['channel_before'] = str(channel_before)
    logs['program_before'] = str(program_before)
    logs['command'] = command
    logs['time_stamp'] = readable_time
    logs['channel_after'] = str(channel_after)
    logs['program_after'] = str(program_after)
    logs['data'] = json.dumps(data)
    logs['profile'] = str(profile)
    interact_logs.append(logs)
"""

def createLog(
    channel_before,  # channel id before interaction
    command,  # command that is being executed
    channel_after,  # channel id after interaction
    result, # result from the command
    profile # current watchprofile
):
    logger.info("Create log for channel information")

    time_stamp = time.time()  # current time stamp
    readable_time = time.ctime(int(time_stamp))  # time in readable format
    logs = {}  # the dictionary for the logs of a single interaction
    logs['channel_before'] = str(channel_before)
    logs['command'] = command
    logs['time_stamp'] = readable_time
    logs['channel_after'] = str(channel_after)
    logs['result'] = str(result)
    logs['profile'] = str(profile)
    interact_logs.append(logs)

#  _     _____ _____
# | |   |  _  |  __ \
# | |   | | | | |  \/
# | |   | | | | | __
# | |___\ \_/ / |_\ \
# \_____/\___/ \____/
#
#
#  _____ _   _______
# |  ___| \ | |  _  \
# | |__ |  \| | | | |
# |  __|| . ` | | | |
# | |___| |\  | |/ /
# \____/\_| \_/___/
#
# ---------------------------------------------------------------
#
# ---------------------------------------------------------------
#  _    _______  ___  ____________ ___________
# | |  | | ___ \/ _ \ | ___ \ ___ \  ___| ___ \
# | |  | | |_/ / /_\ \| |_/ / |_/ / |__ | |_/ /
# | |/\| |    /|  _  ||  __/|  __/|  __||    /
# \  /\  / |\ \| | | || |   | |   | |___| |\ \
#  \/  \/\_| \_\_| |_/\_|   \_|   \____/\_| \_|
#
#
#  _____ _____ ___  ______ _____
# /  ___|_   _/ _ \ | ___ \_   _|
# \ `--.  | |/ /_\ \| |_/ / | |
#  `--. \ | ||  _  ||    /  | |
# /\__/ / | || | | || |\ \  | |
# \____/  \_/\_| |_/\_| \_| \_/
#

#
def escape_sepcial_character(s):
	"""
	Escapes special characters in filenames.
	"""
	value = unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode('ascii')
	value = re.sub(r'[^\w\s-]', '', value.lower())
	return re.sub(r'[-\s]+', '-', value).strip('-_')

def runCommand(
    cmd,  # single command or list of commands
    exe_delay=1,  # delay between funcion calls
    shortinfo=False,  # get a shortened info instead of complete logs
    log=True,  # create log entry
    verbose=True,  # show verbose output
    channelID=0, # optional argument for switchting the channel
    profile=0,
    channelName=""
):
    """
    this function wraps most interactions with the api
    """
    logger.info(f"RunCommand for command {cmd}")
    data = ""
    # check if apis are correctly initited
    if not tv_initiated:
        # if tv is not initiated exit program flow
        logger.error("Error: PyWebOSTV Controls where not properly initiated (see init section).\n")
    else:
        # differentiate between list of commands and single command
        if(type(cmd) is list):
            logger.info("Multliple commands found")
            for single_cmd in cmd:
                # attention! avoid infinite loops here
                runCommand(single_cmd, exe_delay, shortinfo, log, verbose)
        else:
            # create complete log or shortened version
            """
            if(shortinfo):
                logger.info("Create shortened log")
                prog = "get_program_list"
                channel = "get_channel_id"
                info = "get_current_channel_program_info"
            else:
                prog = "get_program_data"
                channel = "get_channel_data"
                info = "get_current_channel_program_info"
            """
            #channel = "get_program_data"
            channel = "get_channel_data"

            # ----- log before
            if log:
                logger.info("Create an event log for current running command(s)")
                if (verbose):
                    logger.info("Creating before-log")
                    #print("creating before-log with commands: " + prog + ", " + channel + "\n")

                # make sure the log call doesn't log itself
                try:

                    channel_before = runCommand(
                        channel, exe_delay, shortinfo, False, verbose, profile, channelName)
                except Exception as e:
                    send_ping("10.42.0.216")
                    logger.warning(f"Error while creating the log with command {channel} as : {e}")
            # ----- log before

            # Init the output string which will be wirtten in the log file
            output_str = ""
            if (verbose):
                # output inbetween interactions...
                print("sleeping for " + str(exe_delay) + " seconds...")
                time.sleep(exe_delay)
                print("done sleeping. executing command: '" + cmd + "'")
            else:
                # some time delay between interactions to avoid flooding the tv with too many requests
                time.sleep(exe_delay)

            # execute the command for controling the tv
            if cmd == "volume_down":  # decreases Volume by 1
                data = tv_media.volume_down()
                output_str = data
            if cmd == "volume_up":  # increases Volume by 1
                data =tv_media.volume_up()
                output_str = data
            if cmd == "mute":  # mutes TV
                data =tv_media.mute(True)
                output_str = data
            if cmd == "unmute":  # unmutes TV
                data =tv_media.mute(False)
                output_str = data
            if cmd == "channel_up":  # increases current channel by 1
                data =tv_control.channel_up()
                output_str = data
                logger.input("Channel up!")
            if cmd == "channel_down":  # decreases current channel by 1
                data =tv_control.channel_down()
                output_str = data
            if cmd == "off":
                data =tv_system.power_off()
                output_str = data
            if cmd == "red":  # pushes red button (HbbTV)
                data =tv_input.red()
                output_str = data
            if cmd == "green":  # pushes green button
                data =tv_input.green()
                output_str = data
            if cmd == "yellow":  # pushes yellow button
                data =tv_input.yellow()
                output_str = data
            if cmd == "blue":  # pushes blue button
                data =tv_input.blue()
                output_str = data
            if cmd == "up":  # pushes up button
                data =tv_input.up()
                output_str = data
            if cmd == "down":  # pushes down button
                data =tv_input.down()
                output_str = data
            if cmd == "left":  # pushes left button
                data =tv_input.left()
                output_str = data
            if cmd == "right":  # pushes right button
                data =tv_input.right()
                output_str = data
            if cmd == "ok":  # pushes ok button
                data =tv_input.ok()
                output_str = data
            if cmd == "back":  # pushes back button
                data =tv_input.back()
                output_str = data
            if cmd == "exit":  # pushes exit button (exits HbbTV)
                data =tv_input.exit()
                output_str = data




            if cmd == "set_channel_with_id": # switch to the channel with the given id
                try:
                    data =tv_control.set_channel_with_id(channelID)
                    output_str = data
                    logger.info(f"Successfully switched to channel {channelID}")
                except IOError as e:
                    send_ping("10.42.0.216")
                    logger.exception(f"Error while switching channel to {channelID}: {e}")




            # Execute the command to get information
            if cmd == "get_channel_data":  # gets raw json-data about the current channel
                # still produces errors on some channels
                try:
                    data =tv_control.get_current_channel()
                    output_str = data
                    logger.info(f"Successfully running command {cmd}")
                except Exception as e:
                    logger.warning(f"Error while using command {cmd} with {e}")
                    #print("---------------\nIOError: " +
                    #      cmd + "\n---------------")
                    send_ping("10.42.0.216")
                    output_str = "'get_channel_data: IOError.'"

            # gets identification number (as string) of current channel
            if cmd == "get_channel_id":
                # still produces errors on some channels
                try:
                    data =tv_control.get_current_channel()["channelNumber"]
                    output_str = data
                    logger.info(f"Successfully running command {cmd}")
                except Exception as e:
                    logger.warning(f"Error while using command {cmd} with {e}")
                    send_ping("10.42.0.216")
                    #print("---------------\nIOError: " +
                     #     cmd + "\n---------------")
                    output_str = "'get_channel_id: IOError.'"

            if cmd == "get_program_data":  # gets raw json-data about current programs
                # still produces errors on some channels
                try:
                    data =tv_control.get_current_program()
                    output_str = data
                    logger.info(f"Successfully running command {cmd}")
                except Exception as e:
                    logger.warning(f"Error while using command {cmd} with {e}")
                    send_ping("10.42.0.216")
                    #print("---------------\nIOError: " +
                     #     cmd + "\n---------------")
                    output_str = "'get_program_data: IOError.'"
            if cmd == "get_program_list":  # gets a neat list of current programs
                # still produces errors on some channels
                try:
                    data = tv_control.get_current_program()["programList"]
                    output_str = data
                    logger.info(f"Successfully running command {cmd}")
                except Exception as e:#
                    logger.warning(f"Error while using command {cmd} with {e}")
                    send_ping("10.42.0.216")
                    #print("---------------\nIOError: " +
                    #      cmd + "\n---------------")
                    output_str = "'get_program_list: IOError.'"


            ########### Get current program ####################################
            if cmd == "get_current_channel_program_info":
                """
                Returns the current program in a json-format output.
                The current program is in the last secition block with:
                {
                 "programId":"",
                 "programName":"",
                 "description":"",
                 "startTime":"",
                 "endTime":"",
                 "localStartTime":"",
                 "localEndTime":"",
                 "duration":,
                 "channelId":"",
                 "channelName":"",
                 "channelNumber":"",
                 "channelMode":""
                }
                The output returns in the second logs.
                """
                try:
                    prog_info = tv_control.get_current_channel_program_info()
                    data =prog_info
                    output_str = prog_info
                    logger.info(f"Successfully running command {cmd}")
                    logger.info(f"Method get_current_channel_program_info returns successfully")
                except Exception as e:
                    logger.warning(f"Error while using command {cmd} with {e}")
                    send_ping("10.42.0.216")
                    #logger.exception(f"IOError in {cmd} with {ioe}")
                    #output_str = "'get_current_channel_program_info: IOError.'"

            ####### Get hardware information ###################################
            if cmd == "get_info":
                """
                Return product information in a json-file format like:
                {
                   "product_name":"",
                   "model_name":"",
                   "sw_type":"",
                   "major_ver":"",
                   "minor_ver":"",
                   "country":"",
                   "country_group":"",
                   "device_id":"",
                   "auth_flag":"",
                   "ignore_disable":"",
                   "eco_info":"",
                   "config_key":"",
                   "language_code":""
                }
                """
                try:
                    info = tv_system.info()
                    data=info
                    output_str = info
                    logger.info(f"Successfully running command {cmd}")
                    #logger.info(f"Method get_info returns: successfully")
                except Exception as e:
                    logger.warning(f"Error while using command {cmd} with {e}")
                    send_ping("10.42.0.216")
                    #logger.exception(f"IOError in {cmd} with {ioe}")
                    output_str = "'get_info: IOERROR.'"
            ####################################################################



            if cmd == "channel_list":  # gets a list of current channels
                try:
                    data=tv_control.channel_list()
                    output_str=data
                    logger.info(f"Successfully running command {cmd}")
                except Exception as e:
                    logger.warning(f"Error while using command {cmd} with {e}")
                    send_ping("10.42.0.216")
                    #print("---------------\nIOError: " +
                     #     cmd + "\n---------------")
                    output_str = "'channel_list: IOError.'"

            if cmd == "screenshot":  # takes a screenshot
                d = tv_control.get_current_program()
                logger.info(f"Get current channel information for channel {channelID}: {d}")

                frq = d['channel']['Frequency']
                signalChID = d['channel']['signalChannelId']
                cName = escape_sepcial_character(channelName)

                screenshotName = channelID +  "+" + str(cName) + "+" + str(frq) + "+" + str(signalChID) + "+" + str(profile) + "+" + str(datetime.datetime.now().strftime("%Y-%m-%d+%H-%M-%S"))+ ".jpg" # no channel name! throws multiple errors or need to work with names ... "_" + channelName +

                try:
                    imageDict = tv_control.execute_Oneshot()
                    data = imageDict
                    output_str = data
                    imagageUri = imageDict["imageUri"]
                    response = urllib.request.urlretrieve(
                        imagageUri, screenshot_folder + screenshotName)
                    output_str = (
                        "Screenshot was taken.\nimage located at: " + str(imagageUri) + "\n")
                    logger.info(f"Screenshot was taken in location {imagageUri}")
                    logger.info(f"Successfully running command {cmd}")
                except Exception as e:
                    #print("Screenshot Error: could not retrieve Image.")
                    output_str = "'screenshot error'"
                    #logger.info(f"Error while taking a screenshot: {e}")
                    logger.warning(f"Error while using command {cmd} with {e}")
                    send_ping("10.42.0.216")

            # create the logs after the interaction
            # ----- log after
            if log:
                #if (verbose):
                    #print("creating after-log with commands: " + prog + ", " + channel + "\n")
                # make sure the log call doesn't log itself
                try:
                    channel_after = runCommand(
                        channel, exe_delay, shortinfo, False, verbose)
                    logger.info(f"Successfully running command {cmd}")
                except Exception as e:
                    logger.warning(f"Error by logging channel after while using command {cmd} with {e}")
                    send_ping("10.42.0.216")
                    #logger.warning(f"Error while ")

                #print(channel_after)
                result = ""
                try:
                    channel_after = channel_after['channelId']
                    channel_before = channel_before['channelId']
                    result = data
                except Exception as e:
                    #print("Screenshot Error: could not retrieve Image.")
                    output_str = "'Log error'"
                    #logger.info(f"Error while taking logs: {e}")
                    logger.warning(f"Error while using command {cmd} with {e}")
                    send_ping("{IP TV}")

                logger.info("Create log!")
                createLog(channel_before,
                          cmd, channel_after, result, profile)
            # ----- log after

            # tell the user about the interaction
            if (verbose):
                print(output_str)
            return output_str


#  _    _______  ___  ____________ ___________
# | |  | | ___ \/ _ \ | ___ \ ___ \  ___| ___ \
# | |  | | |_/ / /_\ \| |_/ / |_/ / |__ | |_/ /
# | |/\| |    /|  _  ||  __/|  __/|  __||    /
# \  /\  / |\ \| | | || |   | |   | |___| |\ \
#  \/  \/\_| \_\_| |_/\_|   \_|   \____/\_| \_|
#
#
#  _____ _   _______
# |  ___| \ | |  _  \
# | |__ |  \| | | | |
# |  __|| . ` | | | |
# | |___| |\  | |/ /
# \____/\_| \_/___/
#
# ---------------------------------------------------------------
#
# ---------------------------------------------------------------
#  _____ _____ ___  ______ _____
# /  ___|_   _/ _ \ | ___ \_   _|
# \ `--.  | |/ /_\ \| |_/ / | |
#  `--. \ | ||  _  ||    /  | |
# /\__/ / | || | | || |\ \  | |
# \____/  \_/\_| |_/\_| \_| \_/
#

def generateInteraction(length):
    """
    Generate a random set of interactions in a given length greater equals 5
    """
    global basepath

    logger.info(f"Generate interaction with {length} interactions")
    up = "up"
    down = "down"
    right = "left"
    left = "right"
    ok = "ok"

    interaction = [up, down, right, left, ok]
    random_interactions = random.choices(interaction, k=length)

    with open(basepath+"interaction_set.txt", "w") as f:
        for int in random_interactions:
            f.write(int)

    return random_interactions


global glob_channelName, glob_channelID
glob_channelName = ""
glob_channelID = ""


def retry_measurement(profil, chID, pos, watchTime, channelName):
    """
    Retry the measurement without metadat
    """
    global screenshot_folder

    logger.info(f"Retry measurement in profile {profil} for channel {channelName} ({chID}) on position {pos}")

    getChannel = False
    program = False
    info = False

    for i in range(10):
        logger.info(f"Try to sample data at loop {i+1}")

        if not getChannel:
            try:
            # try to collect meta data
                tv_control.get_current_channel()
                logger.info(f"Successfully ran command get_current_channel() on loop {i+1}")
            except Exception as e:
                logger.error(f"Error while running command get_current_channel() with {e}")

        if not program:
            try:
                tv_control.get_current_program()
                logger.info(f"Successfully ran command get_current_program() on loop {i+1}")
            except Exception as e:
                logger.error(f"Error while running command get_current_program() with {e}")

        if not info:
            try:
                tv_control.get_current_channel_program_info()
                logger.info(f"Successfully ran command get_current_channel_program_info() on loop {i+1}")
            except Exception as e:
                logger.error(f"Error while running command get_current_channel_program_info() with {e}")

    logger.info("Taking screenshots!")
    # check if screenshots has been taking
    os.chdir(screenshot_folder)
    search = f"{chID}*.jpg"
    files = glob.glob(search)
    captured_screenshots = len(files)

    parsedWatchTime = int(watchTime/60) - captured_screenshots
    logger.info(f"Taking {parsedWatchTime} screenshots")
    for n in range((parsedWatchTime)): # take one screenshot every 1min
        time.sleep(60)
        try:
            logger.info(f"Take a screenshot! in profile: {profil} and channel: {channelName}")
            writeInFile(runCommand("screenshot", profile=profil, channelID=chID, channelName=channelName, shortinfo=False, log=False, verbose=False))
        except Exception as e:
            logger.warning(f"Error while screenshot routine on loop {n} with {e}")





def getChannelName():
    return glob_channelName if glob_channelName else "No channel selected"


def getChannelID():
    return glob_channelID if glob_channelID else "No channel selected"


def programRoutine(chID, profile, channelName):
    """
    Command routine for watching a program.
    """
    try:
        try:
            logger.info(f"Take a screenshot! in profile: {profile} and channel: {channelName}")
            writeInFile(runCommand("screenshot", channelID=chID, profile=profile, channelName=channelName)) # Take a screenshot
        except Exception as e:
            logger.warning(f"Error in program routine while taking a screenshot with {e}")

        try:
            logger.info("Run command: get get_current_channel_program_info")
            writeInFile(runCommand("get_current_channel_program_info")) # get meta data
        except Exception as e:
            logger.warning(f"Error in program routine while running command get_current_channel_program_info with {e}")

        #try:
        #    logger.info("Run command: get_program_data")
        #    writeInFile(runCommand("get_program_data")) # get channel program for the next day
        #except Exception as e:
        #    logger.warning(f"Error in program routine while running command get_program_data with {e}")
    except Exception as e:
        logger.warning(f"Error in Programm routine with {e}")

def getRandomChannelList():
    """
    Get a list of channels to watch in a random order.

    Return dict {Channelname, ChannelID} and the whole channel list for statistics
    """
    logger.info("Get channel list")
    CHANNELLIST: dict={}
    CHANNELLIST_RANDOM: dict={}
    complete_channelList = []

    filter = ["Internet", "connect"]
    ch = runCommand("channel_list", log=False, shortinfo=False, verbose=False) # get list of all available channels
    ch = ch["channelList"] # filter overhead
    logger.info(f"Found {len(ch)} channel")
    for c in ch:
        complete_channelList.append(c)
        ch_ID = c["channelId"]
        ch_name = c["channelName"]
        ch_number = c["channelNumber"]
        is_radio = c["Radio"]
        ch_type = c["channelType"]
        ch_type_id = c["channelTypeId"]
        ch_encrypted = c["scrambled"]
        is_tv = c['TV']

        if ch_encrypted:
            print(f"Channel {ch_name} is encrypted: {ch_encrypted}")
        #logger.info(f"Channel name: {ch_name} -- channel number: {ch_number} -- is radio channel: {is_radio} -- channel type: {ch_type} with id: {ch_type_id}")
        if not is_radio: # filter radio channel

            if "Internet" not in ch_name and "connect" not in ch_name and "conncet" not in ch_name:
                #print(f"Channel: {ch_name} with ID: {ch_ID}")
                CHANNELLIST[ch_ID] = ch_name

    logger.info(f"Found {len(CHANNELLIST.keys())} hbbtv relevant channels")

    filename = '{path}/channellist/'+str(datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))


    with open(filename + '.txt', 'w') as f:
        for k, v in CHANNELLIST.items():
            f.write(f"Channel: {k} - ID: {v}\n")

    with open(filename + '_complete.txt', 'w') as f:
        for channel in complete_channelList:
            f.write(json.dumps(channel))
            f.write("\n")

    #logger.info("Randomize channel")
    for key, value in sorted(CHANNELLIST.items(), key=lambda x: random.random()):
        CHANNELLIST_RANDOM[key] = value

    #logger.info(f"Random channel: {CHANNELLIST_RANDOM}")

    return CHANNELLIST_RANDOM, complete_channelList

def channelStatisic(channelList):
    """
    Return the statisitcs for the channel list.

    e.g. found channel, amount of radio channels, amount of hbbtv, amount of encrypted channels
    """
    radio_channel = 0
    encrypted_channel = 0
    hbbtv_channel = 0
    ipTV = 0

    for channel in channelList:
        if channel["Radio"]: # amount of radio channels
            radio_channel += 1
        elif channel["scrambled"]: # amount of encrypted channels
            encrypted_channel += 1
        elif any(f in channel["channelName"] for f in ["Internet", "Connect"]): # amount of iptv
            ipTV += 1
        else: # if its not filtered by other parameters.... may it´s a hbbtvchannel
            hbbtv_channel +=1


    return radio_channel, encrypted_channel, hbbtv_channel, ipTV

def screenshotRoutine(watchTime, profile, channelName, chID):
    logger.info("Taking screenshots!")
    # added 13.02.2023 JH - not pushed
    parsedWatchTime = int(watchTime/60)
    for n in range((parsedWatchTime)): # take one screenshot every 1min
        logger.info("Sleep 60s")
        time.sleep(60)
        logger.info("Sleep over")
        try:
            logger.info(f"Take a screenshot! in profile: {profile} and channel: {channelName}")
            writeInFile(runCommand("screenshot", profile=profile, channelID=chID, channelName=channelName))
        except Exception as e:
            logger.warning(f"Error while screenshot routine on loop {n} with {e}")

def filterChannelList(list="", order=0):
    global basepath
    if list == "":
        channels= runCommand("channel_list", log=False, shortinfo=False, verbose=False) # get list of all available channels
        channels = channels["channelList"] # filter overhead


        filename = basepath + '/channel/channellist_total_'+str(datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))
        with open(filename + ".txt", "w") as f:
            f.write(json.dumps(channels))


        output = list()
        for channel in channels:
            scrambled = channel["scrambled"]
            chType = channel["channelType"]
            invs = channel["Invisible"]
            chName = channel["channelName"]

            if not scrambled and chType == "Satellite Digital TV" and not invs and chName != "":
                output.append(channel)

        output = sorted(output, key=lambda x:x['channelName']) # need to removed for randomized channel

        with open(basepath + "/channel/channellist.txt", 'w') as f:
            f.write(json.dumps(output))
    else:
        logger.info("Read channellist....")
        try:
            with open("/home/ifis-hbbtv/hbbtv/hbbtv-2022/Code/Measuremement_Framework/remote_control/"+list, "r") as f:
                chlist = (json.loads(f.readline()))
            logger.info("Successfully read channel list")
        except Exception as e:
            logger.warning(e)
        
        try:
            cleaned_list = []
        except Exception as e:
            logger.warning("Error while creating list for cleaned channel",e)

        output = dict()

        for channel in chlist:
            logger.info(f"Check channel")
            channelId = channel['channelId']
            tv_control.set_channel_with_id(channelId)
            time.sleep(10)
            try:
                prog_data = tv_control.get_current_program()

                #print(f"Programlist: {prog_data['programList']:}")
                # Check if empty chlist -> "NoSignal"
                if not prog_data['programList']:
                    
                    logger.warning(f"Remove channel {channelId} for no programlist")
                    chlist.remove(channel)
                else:
                    cleaned_list.append(channel)
                    logger.info(f"Add channel {channelId} to channel list")
            except Exception as e:
                chlist.remove(channel)
                logger.warning(f"Remove channel {channelId} from channel list")



        if order == 1:
            # randomize
            output = sorted(cleaned_list, key=lambda x: random.random())
        elif order == 0:
            output = sorted(cleaned_list, key=lambda x:x['channelName']) # need to removed for randomized channel

        with open(basepath+"/channellist.txt", "w") as f:
            f.write(json.dumps(output))

    return output

def send_ping(target):
    try:
        response_time = ping(target)
        if response_time is not None:
            logger.info(f"Ping response from {target} in {response_time:.2f} ms")
        else:
            logger.warning(f"No response from {target}")
    except Exception as e:
        logger.error(f"Error while pinging {target} - {e}")


def validate_Screenshots(profile, chId):
    """
    Each channel produce 16 or 27 screenshots - based on the profile.
    """
    global screenshot_folder

    os.chdir(screenshot_folder)
    search = f"{chId}*.jpg"
    files = glob.glob(search)
    captured_screenshots = len(files)

    is_valid = False

    if profile == 1:
        number_of_screenshots = 16
        if number_of_screenshots == captured_screenshots:
            is_valid = True
            logger.info(f"For ID: {chId} we captured {captured_screenshots}/{number_of_screenshots}")
        else:
            logger.error(f"Error while captured screenshots in profile {profile} from ID {chId} - ({captured_screenshots}/{number_of_screenshots})")

    elif profile in [3,4,5,6]:
        number_of_screenshots = 27
        if number_of_screenshots == captured_screenshots:
            is_valid = True
            logger.info(f"For ID: {chId} we captured {captured_screenshots}/{number_of_screenshots}")
        else:
            logger.error(f"Error while captured screenshots in profile {profile} from ID {chId} - ({captured_screenshots}/{number_of_screenshots})")


def startInteraction(chlist, profile=1, watchTime=900):
    """
    profile = number of the profile with defaul 1
    watchTime = time to watch in seconds, default 300 (5min)

    Run a few commands to interact with the hbbtv.
    Furthermore run commands to get information about current channels
    e.g.

    The interaction can be one of six different profiles.

    Profiles:
    1. Primetime 20:15 - ein Sender pro Tag
    2. Alle Sender in randomisierter Reihenfolge für n-Minuten schauen
    3. Sender gucken und Button grün drücken
    4. Sender gucken und Button gelb drücken
    5. Sender gucken und Button blau drücken
    6. Sender gucken und Button rot drücken
    """
    global glob_channelID, glob_channelName
    # be sure that TV is on!
    logger.info("Run startInteraction")
    logger.info(f"Measurement for profile {profile} with a watch time of {watchTime} s")

    # Get technical information from the hbbtv - write in a local file
    logger.info("Get technical information from the tv")
    runCommand("get_info", log=False) # todo write in file

    logger.info("Get channel list")
    channels = chlist
    #if restored_channellist == "":
     #   #channels = filterChannelList("channellist_2023_08_24_ground_truth.txt", order=1)
      #  channels = filterChannelList("channellist_09112023-Testlist.txt", order=1)
    #else:
     #   logger.info(f"Load restored channellist: {restored_channellist}")
      #  channels = filterChannelList(restored_channellist, order=1)


    #time.sleep(120)

    #filename = "/home/ifis-hbbtv/hbbtv/01_Code/01_Measuremement_Framework/remote_control/2023-06-07_16-03-39_complete.txt"

    #channels = []
    #with open(filename, 'r') as f:
    #    for line in f:
    #        line = json.loads(line)
    #        if line['TV']:
    #            channels.append(line)

    #channels = sorted(channels, key=lambda x:x['channelName'])

    #already_read_channels = list()
    #with open("/home/ifis-hbbtv/hbbtv/01_Code/02_Data_Analysis/Assist/channellist_31052023_cleaned.txt", 'r') as f:
    #    for line in f:
    #        already_read_channels.append(line.strip())

    #filename = "/home/ifis-hbbtv/hbbtv/01_Code/01_Measuremement_Framework/remote_control/channelList_2023_06_05_validate_cleaned_local_multiple"
    #with open(filename, "r") as f:
    #    for line in f:
    #        channels.append(json.loads(line))
    #
    hbbTVChannelList = {}
    for entry in channels:
        #print(entry)

        chName = entry['channelName']
        chID = entry['channelId']
        #if "/" in chName:
        #if chID not in already_read_channels:
        hbbTVChannelList[chID] = chName

    #channelOrder = hbbTVChannelList.values() # returns the channel id for program order
    #channelStatisic(allChannel) # get statistics about channels

    #hbbTVChannelList = test()

    total_channel_length = len(hbbTVChannelList.keys())
    logger.warning("Found %s channel", total_channel_length)

    logger.info("Init fall back logger")
    #fallbacklog = FallbackLogger(basepath + "fallback.log", hbbTVChannelList, basepath)

    n = 60 # screenshot timer

    i = 0

    if profile == 1:
        # Watch tv for n-sec on each channel, without interaction
        for chID, channelName  in hbbTVChannelList.items():
            #fallbacklog.set_pos_in_list(i)
            logger.info(f"Analyze channel {i+1}/{total_channel_length}: {channelName}")
            logger.info(f"Estimated time till finish: {datetime.timedelta(seconds=(int((total_channel_length-i) * (watchTime))))}")
            i += 1

            retry = False

            try:
                switchChannel(chID, channelName, profile)

                logger.info("Set global parameters")
                glob_channelName = channelName
                glob_channelID = chID

                setChannelNameID()

                logger.info("Going sleep")
                time.sleep(10) # sleep 10s to build the screen
                logger.info("Wake up!")

                send_ping("google.com")
                send_ping("{TV IP}")

                programRoutine(chID, profile, channelName)

                screenshotRoutine(watchTime, profile, channelName, chID)

                logger.info(f"Successfully measur channel: {channelName} with ID: {chID} on position: {i-1}")

                validate_Screenshots(profile, chID)
            except Exception as e:
                logger.error(f"Error while measur channel: {channelName} with ID: {chID} on position: {i-1} with exception: {e}")

                logger.info("Retry...")
                try:
                    retry_measurement(profile, chID, i, watchTime, channelName)
                    retry=True
                except Exception as e:
                    logger.info("Failing retry with {e}")
                #fallbacklog.crash()
            finally:
                if retry:
                    logger.info("Successfully measure channel {channelName} with ID: {chID} in retry")
                else:
                    logger.error(f"Failed to retry measure channel ({channelName}, {chID})")



    elif profile == 2:
        # watch tv at the primetime 20:15 a program, without interaction
        prime_channels_germany = ["ZDF", "ARD", "RTL", "Sat.1", "Vox", "ProSieben", "Kabel eins", "RLTzwei"]
        for channelName, chID in hbbTVChannelList.items():
            switchChannel(chID, channelName, profile)

            glob_channelName = channelName
            glob_channelID = chID

            setChannelNameID()

            time.sleep(10) # sleep 10s to build the screen
            send_ping("google.com")

            programRoutine(chID, profile, channelName)

            screenshotRoutine(watchTime)

            programRoutine(chID, profile, channelName)

    elif profile == 3:
        # Watch tv for n-sec, with interaction
        # Interaction on button
        interaction_Set = generateInteraction(10)

        retry = False

        for chID, channelName  in hbbTVChannelList.items():
            #fallbacklog.set_pos_in_list(i)
            logger.info(f"Analyze channel {i+1}/{total_channel_length}: {channelName}")
            logger.info(f"Estimated time till finish: {datetime.timedelta(seconds=(int((total_channel_length-i) * (watchTime))))}")
            i += 1

            try:
                switchChannel(chID, channelName, profile)

                logger.info("Set global parameters")
                glob_channelName = channelName
                glob_channelID = chID

                setChannelNameID()

                logger.info("Going sleep")
                time.sleep(10) # sleep 10s to build the screen
                logger.info("Wake up!")

                send_ping("google.com")
                send_ping("{TV IP}")

                programRoutine(chID, profile, channelName)

                # execute button routine
                buttonRoutine("red",  profile, channelName, chID, interaction_Set)

                screenshotRoutine(watchTime, profile, channelName, chID)

                logger.info(f"Successfully measur channel: {channelName} with ID: {chID} on position: {i-1}")
            except Exception as e:
                logger.error(f"Error while measur channel: {channelName} with ID: {chID} on position: {i-1} with exception: {e}")

                logger.info("Retry...")
                try:
                    retry_measurement(profile, chID, i, watchTime, channelName)
                    retry=True
                except Exception as e:
                    logger.info("Failing retry with {e}")
                #fallbacklog.crash()
            finally:
                if retry:
                    logger.info("Successfully measure channel {channelName} with ID: {chID} in retry")
                #else:
                 #   logger.error(f"Failed to retry measure channel ({channelName}, {chID})")

    elif profile == 4 :
        # Watch tv for n-sec, with interaction
        # Interaction on button
        interaction_Set = generateInteraction(10)

        retry = False

        for chID, channelName  in hbbTVChannelList.items():
            #fallbacklog.set_pos_in_list(i)
            logger.info(f"Analyze channel {i+1}/{total_channel_length}: {channelName}")
            logger.info(f"Estimated time till finish: {datetime.timedelta(seconds=(int((total_channel_length-i) * (watchTime))))}")
            i += 1

            try:
                switchChannel(chID, channelName, profile)

                logger.info("Set global parameters")
                glob_channelName = channelName
                glob_channelID = chID

                setChannelNameID()

                logger.info("Going sleep")
                time.sleep(10) # sleep 10s to build the screen
                logger.info("Wake up!")

                send_ping("google.com")
                send_ping("{TV IP}")

                programRoutine(chID, profile, channelName)

                # execute button routine
                buttonRoutine("green",  profile, channelName, chID, interaction_Set)

                screenshotRoutine(watchTime, profile, channelName, chID)

                logger.info(f"Successfully measur channel: {channelName} with ID: {chID} on position: {i-1}")
            except Exception as e:
                logger.error(f"Error while measur channel: {channelName} with ID: {chID} on position: {i-1} with exception: {e}")

                logger.info("Retry...")
                try:
                    retry_measurement(profile, chID, i, watchTime, channelName)
                    retry=True
                except Exception as e:
                    logger.info("Failing retry with {e}")
                #fallbacklog.crash()
            finally:
                if retry:
                    logger.info("Successfully measure channel {channelName} with ID: {chID} in retry")
                else:
                    logger.error(f"Failed to retry measure channel ({channelName}, {chID})")

    elif profile == 5:
        # Watch tv for n-sec on each channel, without interaction
        # Interaction on button
        interaction_Set = generateInteraction(10)

        retry = False

        #interaction_Set = ["down","up","up","up","right","up","ok","ok","down","up"]

        for chID, channelName  in hbbTVChannelList.items():
            #fallbacklog.set_pos_in_list(i)
            logger.info(f"Analyze channel {i+1}/{total_channel_length}: {channelName}")
            logger.info(f"Estimated time till finish: {datetime.timedelta(seconds=(int((total_channel_length-i) * (watchTime))))}")
            i += 1

            try:
                switchChannel(chID, channelName, profile)

                logger.info("Set global parameters")
                glob_channelName = channelName
                glob_channelID = chID

                setChannelNameID()

                logger.info("Going sleep")
                time.sleep(10) # sleep 10s to build the screen
                logger.info("Wake up!")

                send_ping("google.com")
                send_ping("{TV IP}")

                programRoutine(chID, profile, channelName)

                # execute button routine
                buttonRoutine("blue", profile, channelName, chID, interaction_Set)


                screenshotRoutine(watchTime, profile, channelName, chID)

                logger.info(f"Successfully measur channel: {channelName} with ID: {chID} on position: {i-1}")
            except Exception as e:
                logger.error(f"Error while measur channel: {channelName} with ID: {chID} on position: {i-1} with exception: {e}")

                logger.info("Retry...")
                try:
                    retry_measurement(profile, chID, i, watchTime, channelName)
                    retry=True
                except Exception as e:
                    logger.info("Failing retry with {e}")
                #fallbacklog.crash()
            finally:
                if retry:
                    logger.info("Successfully measure channel {channelName} with ID: {chID} in retry")
                else:
                    logger.error(f"Failed to retry measure channel ({channelName}, {chID})")


    elif profile == 6:
        # Watch tv for n-sec, with interaction
        # Interaction on button
        interaction_Set = generateInteraction(10)

        retry = False

        for chID, channelName  in hbbTVChannelList.items():
            #fallbacklog.set_pos_in_list(i)
            logger.info(f"Analyze channel {i+1}/{total_channel_length}: {channelName}")
            logger.info(f"Estimated time till finish: {datetime.timedelta(seconds=(int((total_channel_length-i) * (watchTime))))}")
            i += 1

            try:
                switchChannel(chID, channelName, profile)

                logger.info("Set global parameters")
                glob_channelName = channelName
                glob_channelID = chID

                setChannelNameID()

                logger.info("Going sleep")
                time.sleep(10) # sleep 10s to build the screen
                logger.info("Wake up!")

                send_ping("google.com")
                send_ping("{TV IP}")

                programRoutine(chID, profile, channelName)

                # execute button routine
                buttonRoutine("yellow",  profile, channelName, chID, interaction_Set)

                screenshotRoutine(watchTime, profile, channelName, chID)

                logger.info(f"Successfully measur channel: {channelName} with ID: {chID} on position: {i-1}")
            except Exception as e:
                logger.error(f"Error while measur channel: {channelName} with ID: {chID} on position: {i-1} with exception: {e}")

                logger.info("Retry...")
                try:
                    retry_measurement(profile, chID, i, watchTime, channelName)
                    retry=True
                except Exception as e:
                    logger.info("Failing retry with {e}")
                #fallbacklog.crash()
            finally:
                if retry:
                    logger.info(f"Successfully measure channel {channelName} with ID: {chID} in retry")
                else:
                    logger.error(f"Failed to retry measure channel ({channelName}, {chID})")

def turnoff(profile):
    # Measurement done. Turn TV off.
    runCommand("off", log=True, verbose=True, profile=profile)

def switchChannel(chID, chName, profile):
    """
    Switch channel to given channel id.
    """
    logger.info(f"try to switch channel to ID: {chID} - Name: {chName}")
    try:
        writeInFile(runCommand("set_channel_with_id",shortinfo=True, log=True, verbose=True, channelID=chID, channelName=chName, profile=profile)) # Switch channel
        logger.info(f"Successfully switched to channel {chName} with ID {chID} ")
        return
    except IOError as e:
        logger.exception(f"Error while switching to channel {chID}: {e}")


def buttonRoutine(button, profile, channelName, chID, interaction_Set):
    """
    Button routine for given button.
    """
    logger.info(f"Run button routine for {button}-button")
    # Press red-button
    writeInFile(runCommand(button))

    time.sleep(10)

    logger.info(f"Take a screenshot! in profile: {profile} and channel: {channelName}")
    writeInFile(runCommand("screenshot", profile=profile, channelID=chID, channelName=channelName))

    for interaction in interaction_Set:
        logger.info(f"Interaction: {interaction}")
        writeInFile(runCommand(interaction))

        logger.info(f"Take a screenshot! in profile: {profile} and channel: {channelName}")
        writeInFile(runCommand("screenshot", profile=profile, channelID=chID, channelName=channelName))

        time.sleep(5)

    #logger.info(f"Exit {button}-button")
    # Exit button
    #writeInFile(runCommand("exit")) # if its not work use "back"
    #for i in range(20):
    #    writeInFile(runCommand("back"))


def writeInFile(text):
    """
    Write logs in file.
    """
    logger.info(f"Write in file {output_file}")
    with open(output_file, "a") as f:
        #f.write(json.dumps(text))
        #f.write("\n")
        f.write(json.dumps(interact_logs[0]))

        f.write('\n')
        interact_logs.clear()

def finaliseMeasurement():
    logger.info("Finalise measurement into json-file")
    json_logs = json.dumps(interact_logs)
    with open(os.getcwd()+'/measurement_data/test_interact_logs.json', 'w') as f:
        json.dump(json_logs, f, ensure_ascii=False)
    with open(os.getcwd()+'/measurement_data/test_interact_logs_'+ str(ts) +'.json', 'w') as f:
        json.dump(json_logs, f, ensure_ascii=False)

def setChannelNameID():
    global glob_channelID, glob_channelName
    logger.info(f"Write channelname: {glob_channelName} and channel ID: {glob_channelID}")
    with open("{path}/unfug.txt", "w") as f:
        f.write(f"{getChannelName()};{getChannelID()}")

def validateHbbTVChannel():
    global glob_channelID, glob_channelName
    """
    Validate a channel if its hbbtv or not.
    """
    ch = runCommand("channel_list", log=False, shortinfo=False, verbose=False) # get list of all available channels
    ch = ch["channelList"] # filter overhead

    i = 1

    log = []
    log_data = []
    # iterate over the channel list
    for c in ch:
        logger.info(f"Read channel {i} from {len(ch)}")
        i += 1

        log.append(c) # store all channel data in an array

        # get channel meta information
        ch_ID = c["channelId"] # channel id to switch throught it with set_channel_with_id()
        ch_name = c["channelName"] # channel name

        logger.info(f"Set global channel name to: {ch_name}")
        glob_channelName = ch_name

        logger.info(f"Set global channel ID to: {ch_ID}")
        glob_channelID = ch_ID

        is_radio = c["Radio"] # is the current channel a radio channel -> true

        # filter radio channel
        if not is_radio:
            # switch to the channel
            runCommand("set_channel_with_id",False, False, False, channelID=ch_ID) # no logging, verbose or shortinfo needed
            data = runCommand("get_channel_data", shortinfo=False, log=False, verbose=False) # get data from the channel
            logger.info(f"data: {data}")
            log_data.append(data)

            # watch 60s tv to generate traffic
            parsedWatchTime = int(300/60)
            for n in range((parsedWatchTime)): # take one screenshot every 2min
                time.sleep(60)
                logger.info(f"Take a screenshot! in profile: VALIDATION and channel: {channelName}")
                writeInFile(runCommand("screenshot", False, False, False, profile="VALIDATION", channelName=ch_name))


    user_input = input("Disconnected from the internet? (y/n) ")
    if user_input == "y":
        logger.info("Stat validation process 2")
        with open(os.getcwd() + "/channel_validation_channel_list.txt", "w") as f:
            for entry in log:
                f.write(json.dumps(entry))
                f.write("\n")
    else:
        logger.error("Close program")

    user_input = input("Disconneted satellite and connected internet? (y/n)")
    if user_input == "y":
        logger.info("Start validation process 3")
        with open(os.getcwd() + "/channel_validation_channel_data.txt", "w") as f:
            for entry in log_data:
                f.write(json.dumps(entry))
                f.write("\n")
    else:
        logger.error("Close program")



def test():
    ch = runCommand("channel_list", log=False, shortinfo=False, verbose=False) # get list of all available channels
    ch = ch["channelList"] # filter overhead

    output = []
    hbbtv_channel = {}
    ch_radio = 0
    with open(os.getcwd() + "/channel_data_all.txt", "w") as f:
        for c in ch:
            ch_ID = c["channelId"]
            ch_name = c["channelName"]
            ch_number = c["channelNumber"]
            is_radio = c["Radio"]
            if is_radio:
                ch_radio += 1
            ch_type = c["channelType"]
            ch_type_id = c["channelTypeId"]
            ch_encrypted = c["scrambled"]
            is_tv = c['TV']

            ch_inf = [ch_ID, ch_name, ch_number, is_radio, is_tv, ch_encrypted]


            if not is_radio:
                runCommand("set_channel_with_id",False, False, False, channelID=ch_ID)
                time.sleep(120)
                data = runCommand("get_channel_data", shortinfo=False, log=False, verbose=False)
                ch_inf.append(data['hybridtvType'])



                if data['hybridtvType'] == "HBBTV":
                    hbbtv_channel[ch_name] = ch_ID

                f.write(json.dumps(data))
                f.write("\n\n")

            output.append(c)

    logger.info(f"Found {len(ch)} channel")
    logger.info(f"Found {ch_radio} radio channel")
    logger.info(f"Found {len(ch) - len(hbbtv_channel)} channel without hbbttv flag")
    logger.info(f"Found {len(hbbtv_channel)} channel with hbbtv flag")


    #return hbbtv_channel

    #with open(os.getcwd() + "/channel_data.txt", "w") as f:
    #    f.write("ChannelID,ChannelName;ChannelNumber;isRadio;isTV,isEncrypted;HbbTVType")
    #    for data in output:
    #        f.write(f"{data[0]};{data[1]};{data[2]};{data[3]};{data[4]};{data[5]};{data[6]}\n")

def restoreMeasurement():
    logger.info("Restore measurement")
    global basepath, struct_meta, struct_screenshots
    global output_file
    global screenshot_folder
    # Output file for the session
    basepath, struct_meta, struct_screenshots = createDataStructure(True)
    output_file = os.listdir(struct_meta) # should return only one doc!
    screenshot_folder = struct_screenshots

    try:
        tv_init()
    except:
        logger.error(f"Error while Initialize tv")

    channellist = dict()
    with open(basepath + "res_channellist.txt", "r") as f:
        channellist = json.loads(f.readline())

    #startInteraction(profile=1,watchTime=60, channellist)


def start():
    logger.info("Start Program")
    global basepath, struct_meta, struct_screenshots
    global output_file
    global screenshot_folder

    try:
        tv_init()
    except:
        logger.error(f"Error while Initialize tv")

    logger.info("Build structure")
    try:
        basepath, struct_meta, struct_screenshots = createDataStructure()
        output_file = struct_meta + 'Metadata_' + str(datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")) + '.txt'
        screenshot_folder = struct_screenshots
        logger.info("Successfully build folder strcuture")
    except:
        logger.error(f"Error while building folder strcuture")

    start_measurement = input("Type 1 for start (turn mitm proxy on before...)")
    if str(start_measurement) == "1":
        startInteraction(profile=1,watchTime=900)
    #finaliseMeasurement()

#
#  _____ _   _______
# |  ___| \ | |  _  \
# | |__ |  \| | | | |
# |  __|| . ` | | | |
# | |___| |\  | |/ /
# \____/\_| \_/___/
#
# ---------------------------------------------------------------

if __name__ == "__main__":
    start()



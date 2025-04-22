import pprint
import json
from push_ops import stream_to_BQ
import os
import pickle
import logging
import sys

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

PICKLE_FILE = os.path.join("resources", "combined_lang_data.pkl")
# INPUT_FILE = os.path.join("00_Data", "channellist", "channel_list2023-03-17_14-57-15_complete.txt")
ANALYZED_FILE = os.path.join("resources", "analyzed_channels.txt")


def read_channel_data():
    all_channels = []
    data_path = os.path.join(os.getcwd(), "00_Data", "channellist")
    # F:\hbbtv - 2022\Code\Data_Analysis\DatabasePusher\00    _Data
    print(data_path)
    channel_files = os.listdir(data_path)
    logger.info(f"Found {len(channel_files)} channel files")
    for file in channel_files:
        with open(os.path.join(data_path, file), 'r') as f:
            for line in f:
                # all_channels.append(json.loads(line))
                all_channels = json.loads(line)

    return all_channels


def write_channel_details(measurement_id):
    all_channels = read_channel_data()

    # Load country data for the channels
    with open(PICKLE_FILE, 'rb') as f:
        combined_lang_data = pickle.load(f)
    # pp = pprint.PrettyPrinter(indent=4)
    # pp.pprint(combined_lang_data)

    # Load analyzed channels
    analyzed_channels = get_analyzed_channels()

    # Reformat all channels so that they can be pushed to BigQuery
    for channel in all_channels:
        # print(len(channel))
        # for c in channel:
        #     print(c)
        # sys.exit(0)
        if 'favoriteGroup' in channel:
            channel['favoriteGroup'] = str(channel['favoriteGroup'])
        if 'CASystemIDList' in channel:
            channel['CASystemIDList'] = str(channel['CASystemIDList'])
        if 'groupIdList' in channel:
            channel['groupIdList'] = str(channel['groupIdList'])

        # TODO adjust once we know which one to analyze...

        lower_name = channel['channelName'].lower()
        channel['analyzed'] = None #lower_name in analyzed_channels
        # Map (guess) language of the channel.

        channel['country'] = country_mapping(lower_name, combined_lang_data, channel)
        channel['scan_profile'] = measurement_id

    # Write data to big query DB
    stream_to_BQ('hbbtv-research.hbbtv.channel_details', all_channels)


def get_analyzed_channels():
    analyzed_channels = set()

    with open(ANALYZED_FILE, 'r', encoding="utf8") as f:
        for line in f:
            channel = line.split(" - ")[0].replace("Channel: ", "").lower()
            analyzed_channels.add(channel)
    return analyzed_channels


def country_mapping(lower_name, combined_lang_data, channel):
    """
    Maps a country to a channel.

    :param lower_name: the name (in lower case) of the channel
    :param combined_lang_data: The mapping object (dict)
    :param channel: additional data to the channel
    :return: An educated guess of the channels country -- based on data found online.
    """

    country = None
    if lower_name in combined_lang_data:
        country = combined_lang_data[lower_name]
        if len(country) == 1:
            country = next(iter(country))
        else:
            country = "multiple languages"

    # Manual classification if the name is not present on the websites
    if country is None or country == 'unknown':
        if lower_name.startswith('ndr') or lower_name.startswith('swr') or lower_name.startswith('wdr') \
                or lower_name.startswith('br') or lower_name.startswith('mdr') or lower_name.startswith('rbb') \
                or lower_name.startswith('ard') or lower_name.startswith('zdf') or lower_name.startswith('hr-f') \
                or lower_name.startswith('orf') or lower_name.startswith('rfo'):
            country = "german"
        elif lower_name.endswith('austria') or lower_name.endswith('deutschland') or lower_name.endswith(
                ' d') or lower_name.endswith(' at') or lower_name.endswith(' a'):
            country = "german"
        elif lower_name.startswith('prosieben') or lower_name.startswith('rtl2') or lower_name.startswith(
                'rtlzwei') or lower_name.startswith('pro7') or lower_name.startswith('sat.1') or lower_name.startswith(
            'kabel eins') or lower_name.startswith('qvc') or lower_name.startswith('sixx') or lower_name.startswith(
            'vox'):
            country = "german"
        elif lower_name.replace(' hd', '').strip() in combined_lang_data or lower_name.replace(' uhd', '') in combined_lang_data \
                or lower_name.replace(' sd', '') in combined_lang_data or lower_name + " hd" in combined_lang_data:
            country = "german"
        elif 'deutschland' in lower_name or 'austria' in lower_name or 'german' in lower_name or 'schweiz' in lower_name or 'oesterreich' in lower_name:
            country = "german"
        elif lower_name.startswith('cnn') or 'english' in lower_name or 'intl' in lower_name or lower_name.startswith('cnbc') \
                or lower_name == 'shoplc hd' or lower_name == 'mm1 service 1':
            country = "english"
        elif 'francais' in lower_name or 'french' in lower_name or 'francis' in lower_name or lower_name == 'tv5 monde'\
                or lower_name == 'canal algerie' or lower_name == 'barker collectivites sd4' or lower_name == 'canal sur a.'\
                or lower_name == 'canal algerie' or lower_name == 'canal algerie':
            country = "french"
        elif lower_name.startswith('ses ') or lower_name.startswith('pace tds ') or lower_name.startswith('service 1') \
                or lower_name.startswith('service13405') or lower_name.startswith('humax pr-hd3000s'):
            country = "no channel"
        elif channel['Radio']:
            country = "RADIO"
        elif lower_name == '' or lower_name == '.' or 'test' in lower_name:
            country = "no name"
        elif lower_name == 'channel21' or lower_name.startswith('tagesschau24') or lower_name == 'sport1' \
                or lower_name.startswith('dreamgirls24') or lower_name.startswith('erotiksat24') or lower_name == 'maennersache tv'\
                or lower_name == 'fotohandy' or lower_name == 'sex-kontakte' or lower_name == 'volksmusik' \
                or lower_name == 'babestation24' or lower_name == 'bunnyclub24' or lower_name == 'erotika tv - neu!' \
                or lower_name.startswith('tv.ingolstadt') or lower_name == 'wir24tv' or lower_name == 'volksmusik.tv'\
                or lower_name == 'rhein main tv' or lower_name == 'ewtn katholisches tv' or lower_name == 'mediashop- meine einkaufswelt' \
                or lower_name == 'rtl regional nrw' or lower_name == 'rtl hb nds' or lower_name == 'mediashop- neuheiten' \
                or lower_name.startswith('channel21') or lower_name == 'hgtv' or lower_name.startswith('a.tv') \
                or lower_name == 'krone.tv' or lower_name == 'juwelotv' or lower_name == 'visit-x.tv' \
                or lower_name.startswith('pearl.tv') or lower_name == 'tv1 ooe neu' or lower_name == 'dateline'\
                or lower_name == 'megaradiomix' or lower_name == 'melodie tv neu' or lower_name == 'anixe+' \
                or lower_name == 'nick/comedy central+1' or lower_name == 'mei musi' or lower_name == 'ntv'\
                or lower_name == 'lt1-ooe' or lower_name == 'wedo movies' or lower_name == 'folx music television' \
                or lower_name == 'nitro' or lower_name == 'genius plus':
            country = "german"
        elif lower_name.startswith('aragon tv') or lower_name == 'promo tsa' or lower_name.startswith('extremadura') \
                or lower_name.startswith('tvga hd europa'):
            country = "spanish"
        elif lower_name.startswith('cgtn news hd'):
            country = "chinese"
        elif lower_name.startswith('nhk world-jpn'):
            country = "japanese"
        elif lower_name.startswith('ua:pershyi'):
            country = "ukrainian"
        elif lower_name.startswith('bvn tv'):
            country = "dutch"
        elif lower_name.startswith('arirang  hd'):
            country = "korean"
        else:
            # print(lower_name, "not in data")
            country = "unknown"
            # country = "multiple languages"

    return str(country)

import pandas as pd
from google.cloud import bigquery
import os
from tabula.io import read_pdf
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
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "..", "DatabasePusher", "resources", "google_bkp.json")
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


def get_all_channels():
    """
    Returns of channel names observed in the measurement.
    """

    # How many tracking pixel did we observe?
    result_df_pixel = exec_select_query("""SELECT DISTINCT(channelName) as channel_name
                                            FROM `hbbtv-research.hbbtv.channellist`;""")

    return set(result_df_pixel['channel_name'])


def read_eutelsat_list(input_channels):
    """
    Infers a channel's category (e.g., children or sport) for channels transmitted via Eutelsat satellites.
    :param input_channels: List of channel names
    :return: Dict with name to category mapping.
    """
    # Read Eutesat channel detail list
    eutelsat_file_path = os.path.join(os.getcwd(), "resources", "Tv_LineUp.csv")
    channel_details = pd.read_csv(eutelsat_file_path)[['Channel', 'Genre']].set_index('Channel')
    channel_details = channel_details.to_dict()['Genre']
    channel_details = {k.lower(): v for k, v in channel_details.items()}

    # Iterate over analyzed channels and map categories or None if they are not present.
    return get_channel_mapping(input_channels, channel_details)


def read_astra_list(input_channels):
    """
    
    :param input_channels: 
    :return: 
    """
    full_channel_details = dict()
    channel_category = dict()

    # Read the tables from the PDF
    pdf_file = os.path.join(os.getcwd(), "resources", "all_channels_astra.pdf")
    tables = read_pdf(pdf_file, pages='1-23')

    # Extract data from the table(s)
    for table in tables:
        for row in table.iterrows():
            c_name = row[1]['Name'].lower()
            c_type = row[1]['Type']
            c_theme = row[1]['Genre']
            c_lang = row[1]['Sprache']
            c_orbital_position = row[1]['Orb Pos.']
            c_enc = row[1]['Verschlüsselungssystem']
            c_quality = row[1]['Qualität']
            c_satellite = row[1]['Satellit']
            c_transponder = row[1]['Transponder']
            c_pol = row[1]['Pol.']
            c_sr = row[1]['SR']
            c_frequency = row[1]['Frequenz']
            c_paket = row[1]['Paket / frei empfangbar']
            c_mpeg = row[1]['MPEG-2/MPEG-4']

            if c_name not in full_channel_details:
                full_channel_details[c_name] = set()
                channel_category[c_name] = set()
                full_channel_details[c_name].add((c_paket, c_theme, c_orbital_position, c_satellite, c_lang,
                                                            c_quality, c_enc, c_transponder, c_pol, c_mpeg, c_type,
                                                            c_frequency, c_sr))
                channel_category[c_name].add(c_theme)
            else:
                full_channel_details[c_name].add(
                    (c_paket, c_theme, c_orbital_position, c_satellite, c_lang, c_quality, c_enc,
                     c_transponder, c_pol, c_mpeg, c_type, c_frequency, c_sr))
                channel_category[c_name].add(c_theme)

    # Find channel categories for Astra channels
    return get_channel_mapping(input_channels, channel_category)


def get_channel_mapping(input_channels, channel_category):
    channel_category_mapping = dict()

    for channel_name in input_channels:
        channel_name = channel_name.lower()
        if channel_name not in channel_category_mapping:
            channel_category_mapping[channel_name] = set()

        if channel_name in channel_category:
            # print(channel_category[channel_name])
            value = channel_category[channel_name]
            if not isinstance(value, set):
                value = str(value)
                value = {value}
            if value == {'nan'} or value == {'null'}:
                value = {"Unknown"}
            channel_category_mapping[channel_name].update(value)
        else:
            # Test if a channel with a similar name exists
            categories, found_category = heuristic_channel_mapping(channel_name, channel_category)

            # If we still found no channel category, we add None.
            if not found_category:
                channel_category_mapping[channel_name].add(None)
            else:
                channel_category_mapping[channel_name].update(categories)

    return channel_category_mapping


def heuristic_channel_mapping(channel_name, channel_categories):
    categories = set()
    found_category = False

    channel_name_tmp = channel_name.replace('uhd', '').replace('hd', '').replace('_', ' ').replace('.', ' ').strip()
    channel_name_tmp = " ".join(channel_name_tmp.split())

    # Some very heuristic replaces to cope with the difference between the channels names captured by the TV and the
    # ones listed online.
    if channel_name_tmp.startswith("wdr köln"):
        channel_name_tmp = "wdr fernsehen köln"
    elif channel_name_tmp.startswith("wdr"):
        channel_name_tmp = channel_name_tmp.replace("wdr", "wdr studio")
        channel_name_tmp = " ".join(channel_name_tmp.split())
    elif channel_name_tmp.startswith("radio bremen"):
        channel_name_tmp = "radio bremen"
    elif channel_name_tmp.startswith("rtl"):
        channel_name_tmp = "rtl"
    elif channel_name_tmp.startswith("handystar"):
        channel_name_tmp = "handystar"
    elif channel_name_tmp.startswith("br"):
        channel_name_tmp = "bayerisches fernsehen nord"
    elif channel_name_tmp.startswith("ndr"):
        channel_name_tmp = "ndr fernsehen"
    elif channel_name_tmp.startswith("mdr"):
        channel_name_tmp = "mdr fernsehen"
    elif channel_name_tmp.startswith("swr"):
        channel_name_tmp = "swr fernsehen rheinland-pfalz"
    elif channel_name_tmp.startswith("bbc"):
        channel_name_tmp = "bbc world news europe"
    elif channel_name_tmp.startswith("kabel1"):
        channel_name_tmp = channel_name_tmp.replace("kabel1", "kabel eins")
        channel_name_tmp = " ".join(channel_name_tmp.split())
    elif channel_name_tmp.startswith("kabel 1"):
        channel_name_tmp = channel_name_tmp.replace("kabel 1", "kabel eins")
        channel_name_tmp = " ".join(channel_name_tmp.split())
    elif channel_name_tmp.startswith("sat 1"):
        channel_name_tmp = "sat. 1 deutschland"
    elif channel_name_tmp.startswith("zdfinfo"):
        channel_name_tmp = "zdf info"
    elif channel_name_tmp.startswith("sport1"):
        channel_name_tmp = "sport 1"
    elif channel_name_tmp.startswith("sky sport bundesliga"):
        channel_name_tmp = "sky sport bundesliga"
    elif channel_name_tmp.startswith("nick/comedy central"):
        channel_name_tmp = "comedy central"
    elif channel_name_tmp.startswith("anixe"):
        channel_name_tmp = "anixe +"
    elif channel_name_tmp.startswith("juwelotv"):
        channel_name_tmp = "juwelo"
    elif channel_name_tmp.startswith("mediashop"):
        channel_name_tmp = "mediashop"
    elif channel_name_tmp.startswith("pro7"):
        channel_name_tmp = "prosieben deutschland"
    elif channel_name_tmp.startswith("melodie tv"):
        channel_name_tmp = "melodie tv"
    elif channel_name_tmp.startswith("sky sport austria"):
        channel_name_tmp = "sky sport austria"
    elif channel_name_tmp.startswith("hr-fernsehen"):
        channel_name_tmp = "hr fernsehen"
    elif channel_name_tmp.startswith("rhein main tv"):
        channel_name_tmp = "rheinmain"
    elif channel_name_tmp.startswith("hse trend"):
        channel_name_tmp = "hse trend"
    elif channel_name_tmp.startswith("r9 oesterreich"):
        channel_name_tmp = "r9"
    elif channel_name_tmp.startswith("folx music television"):
        channel_name_tmp = "folx music"

    elif channel_name_tmp.startswith("shoplc"):
        channel_name_tmp = "shop lc"
    elif channel_name_tmp.startswith("hse"):
        channel_name_tmp = "hse"
    elif channel_name_tmp.startswith("folx music television"):
        channel_name_tmp = "folx music"
    elif channel_name_tmp.startswith("folx music television"):
        channel_name_tmp = "folx music"

    # Find the channel in the channel list
    for channel_in_list, current_category in channel_categories.items():
        if not isinstance(current_category, set):
            current_category = str(current_category)
            current_category = {current_category}
        if current_category == {'nan'} or current_category == {'null'}:
            current_category = {"Unknown"}

        # Check if the adjusted channel name is equal to the channel name
        if channel_name_tmp == channel_in_list:
            # If we find an exact match, we only use this category
            categories = set()
            categories.update(current_category)
            return categories, True
        # Check if channel same is similar (i.e., contains the search term)
        elif channel_name_tmp in channel_in_list:
            categories.update(current_category)
            found_category = True

    return categories, found_category


def combine_satellite_mapping(eutelsat_mapping, astra_mapping):
    final_channel_category_mapping = dict()

    # Merge the two dictionaries.
    for channel_name, category in eutelsat_mapping.items():
        if channel_name not in final_channel_category_mapping:
            final_channel_category_mapping[channel_name] = set()

        final_channel_category_mapping[channel_name].update(category)

    for channel_name, category in astra_mapping.items():
        if channel_name not in final_channel_category_mapping:
            final_channel_category_mapping[channel_name] = set()
        final_channel_category_mapping[channel_name].update(category)

    # Clean the dict
    i = 0
    for key, value in final_channel_category_mapping.items():
        # print(key, value)
        if len(value) > 1:
            value.discard(None)
        if value == {None}:
            i += 1
            print("No Category:", key)
    print("missing", i)
    return final_channel_category_mapping


def translate_mapping(merged_mapping):
    translated_category_mapping = dict()
    for channel, categories in merged_mapping.items():
        translated_category_mapping[channel] = set()
        for category in categories:
            if category == 'Allgemeine Unterhaltung':
                translated_category_mapping[channel].update({'General'})
            elif category == 'Dokumentationen':
                translated_category_mapping[channel].update({'Documentary'})
            elif category == 'Kinder':
                translated_category_mapping[channel].update({'Children'})
            elif category == 'Musik':
                translated_category_mapping[channel].update({'Music'})
            elif category == 'Musik':
                translated_category_mapping[channel].update({'Music'})
            elif category == 'Filme':
                translated_category_mapping[channel].update({'Movies'})
            elif category == 'Erwachsene':
                translated_category_mapping[channel].update({'Adult'})
            elif category == 'Nachrichten':
                translated_category_mapping[channel].update({'News'})
            elif category == 'Reise':
                translated_category_mapping[channel].update({'Travel'})
            elif category == 'Promo':
                translated_category_mapping[channel].update({'Promotions'})
            elif category == 'Politik':
                translated_category_mapping[channel].update({'Politics'})
            elif category == 'Kultur':
                translated_category_mapping[channel].update({'Culture'})
            elif category == 'Sport':
                translated_category_mapping[channel].update({'Sports'})
            else:
                translated_category_mapping[channel].update({category})
    return translated_category_mapping


def update_channel_categories_in_db(mapping):
    for channel, categories in mapping.items():
        query = f""" UPDATE `hbbtv.channellist`
                       SET channel_category=\"{categories}\"
                       WHERE LOWER(channelName)=LOWER('{channel}');"""
        exec_update_query(query)


def clean_mapping(mapping):
    for channel, categories in mapping.items():
        if len(categories) >1:
            categories.discard('Unknown')
            categories.discard(None)
            categories.discard('null')

    return mapping


if __name__ == '__main__':
    # Read analyzed channels
    all_channels = get_all_channels()

    # Map names to categories
    eutelsat_mapping = read_eutelsat_list(all_channels)
    astra_mapping = read_astra_list(all_channels)

    # Merge mappings
    full_merged_mapping = combine_satellite_mapping(eutelsat_mapping, astra_mapping)
    translated_mapping = translate_mapping(full_merged_mapping)
    final_mapping = clean_mapping(translated_mapping)

    # Write channel categories to DB
    update_channel_categories_in_db(translated_mapping)

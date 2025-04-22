#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import tldextract
import json
import urllib
import sys
import ast
from haralyzer import HarParser
from push_ops import stream_to_BQ, exec_BQ_rows
from postgres_ops import write_requests, write_responses
from adblockparser import AdblockRules
import os
import logging
from datetime import datetime, timedelta

# Each log line includes the date and time, the log level, the current function and the message
formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(funcName)-30s %(message)s')
# The log file is the same as the module name plus the suffix ".log"
# i.e.: calculate.py -> calculate.py.log
fh = logging.FileHandler("%s.log" % (os.path.join("logs", os.path.basename(__file__))))
sh = logging.StreamHandler()
fh.setLevel(logging.DEBUG)  # set the log level for the log file
fh.setFormatter(formatter)
sh.setFormatter(formatter)
sh.setLevel(logging.INFO)  # set the log level for the console
logger = logging.getLogger(__name__)
logger.addHandler(fh)
logger.addHandler(sh)
logger.setLevel(logging.DEBUG)
logger.propagate = False


KEYWORDS_COOKIE_BANNERS = [u"ich stimme zu", u"cookies akzeptieren", u"i accept", u"accept", u"cookies zulassen",
                               u"jag accepterar alla cookies", u"yes, i'm happy", u"accept all cookies",
                               u"accepter", u"i agree", u"accept all", u"agree", u"zaakceptui wszystjie pliki cookie",
                               u"alle akzeptieren", u"aceptar cookies", u"accepter & fermer", u"einwilligung"
                               u"alle cookies akzeptieren", u"hyväksy kaikki", u"zustimmen", u"alle cookies aktivieren"
                               u"erforderliche und optionale cookies erlauben", u"allow essential and optional cookies",
                               u"jag accepterar alla cookies", u"zulassen", u"accept cookies",
                               u"accetta tutti i cookie", u"tout accepter", u"weiter mit den empfohlenen cookies",
                               u"got it", u"agree and access site", u"yes i agree", u"alle cookies zulassen",
                               u"accept & continue", u"allow all cookies", u"got it!", u"agree & close",
                               u"aceptar todas", u"rozumim", u"jag förstår", u"ok, agreed", u"annehmen",
                           u"alle zulassen", u"aceptar todas las cookies", u"akzeptieren & schließen",
                           u"hyväksy kaikki evästeet", u"save and close", u"accept additional cookies",
                           u"allow all", u"all ok", u"ok, akzeptiere alles", u"terima", u"přijmout", u"consent",
                           u"agree and close", u"accept settings", u"verstanden!", u"acepto", u"allow & close",
                           u"allow all cookies", u"wählen sie alle", u"accept and close", u"cookies erlauben",
                           u"ok, i understand", u"yes", u"prihvati i zatvori", u"enable all", u"i understand",
                           u"alle annehmen", u"continue to site", u"ok, verstanden",
                           u"ich stimme der verwendung von cookies zu", u"accepter la sélection", u"zustimmen",
                           u"continue", u"d'accord", u"allow cookies", u"aceitar todos", u"aceptar",
                           u"permitir todas las cookies", u"accepter tout", u"accept recommended settings",
                           u"einwilligen mit personalisierung", u"ok, i agree", u"j'accepte", u"accepter et fermer",
                           u"utilizar cookies opcionales", u"accetto", u"alle cookies gestatten",
                           u"i consent to cookies", u"okay, thanks", u"ok, tout accepter",
                           u"cookies akzeptieren und schließen", u"zgadzam się", u"entendi", u"przejdź do serwisu",
                           u"akceptuję i przechodzę do serwisu", u"verstanden", u"akzeptieren"]

KEYWORDS_PRIVACY_POLICY = [u"поверителност", u"политика за данни", u"политика лд", u"лични данни", u"бисквитки",
                               u"условия", u"soukromí", u"používání dat", u"ochrana dat", u"osobních údajů", u"cookie",
                               u"personlige oplysninger", u"datapolitik", u"privatliv", u"personoplysninger",
                               u"regler om fortrolighed", u"personlige data", u"persondata", u"datenschutz",
                               u"datenrichtlinie", u"privatsphäre", u"απορρήτου", u"απόρρητο", u"προσωπικά δεδομένα",
                               u"εμπιστευτικότητας", u"ιδιωτικότητας", u"πολιτική δεδομένων", u"προσωπικών δεδομένων",
                               u"privacy", u"data policy", u"data protection", u"privacidad", u"datos personales",
                               u"política de datos", u"privaatsus", u"konfidentsiaalsus", u"isikuandmete",
                               u"andmekaitse",
                               u"küpsis", u"yksityisyy", u"tietokäytäntö", u"tietosuoja", u"henkilötie", u"eväste",
                               u"confidentialite", u"confidentialité", u"vie privée", u"vie privee",
                           u"données personnelles",
                           u"donnees personnelles", u"utilisation des données", u"utilisation des donnees", u"rgpd",
                           u"príobháideach", u"cosaint sonraí", u"cosanta sonraí", u"fianáin", u"fianán",
                           u"privatnost",
                           u"osobnih podataka", u"upotrebi podataka", u"zaštita podataka", u"obradi podataka",
                           u"kolačić",
                           u"adatvédel", u"adatkezel", u"személyes adatok védelme", u"riservatezza", u"privatezza",
                           u"dati personali", u"privātum", u"sīkdat", u"privatum", u"konfidencialumas",
                           u"asmens duomenų",
                           u"duomenų sauga", u"slapuk", u"gegevensbescherming", u"gegevensbeleid", u"prywatnoś",
                           u"dane osobowe", u"przetwarzanie danych", u"zasady przetwarzania danych",
                           u"zasady dotyczące danych", u"ochrona danych", u"privacidade", u"dados pessoais",
                           u"política de dados", u"rpgd", u"direitos do titular dos dados", u"confidențialitate",
                           u"confidentialitate", u"protecția datelor", u"súkromi", u"využívania údajov",
                           u"ochrana údajov",
                           u"osobných údajov", u"zásady ochrany osobných", u"osobné údaje", u"gdpr", u"zasebnost",
                           u"osebnih podatkov", u"piškotki", u"varstvo podatkov", u"sekretess", u"datapolicy",
                           u"personuppgifter", u"integritet", u"kakor", u"informationskapslar", u"policy", ]


def write_har_files(measurement_id, request_id = 0):
    logger.info("Start process har from files")
    # TODO: Path has to be changed from one file to a folder with multiple files
    path_har = os.path.join(os.getcwd(), "00_Data", "hardump")
    # path_har = "C:/Users/boett/Documents/GitHub/hbbtv-2022/03_results/evaluation_data/Har-Dump 2023-06-02/"
    har_files = os.listdir(path_har)
    logger.info(f"Found {len(har_files)} har files!")

    for file in har_files:
        request_id, requests, responses, cookies = get_http_objects(os.path.join(path_har, file), measurement_id, request_id)
        # print(request_id)
        # Write to BigQuery database
        # write_requests(requests)
        # write_responses(responses)
        logger.info(f"Requests: {len(requests)}, Responses: {len(responses)}, Cookies: {len(cookies)}")
        stream_to_BQ('hbbtv-research.hbbtv.requests', requests)
        stream_to_BQ('hbbtv-research.hbbtv.responses', responses)
        stream_to_BQ('hbbtv-research.hbbtv.cookies', cookies)


def process_har(har_file):
    logger.info(f"Process for file: {har_file}")
    har_parser = HarParser.from_file(har_file)
    har_data = har_parser.har_data
    har_data['entries'] = [entry for entry in har_data['entries']]

    return har_data


def get_http_objects(har_file, measurement_id, request_id=0):
    har_data = process_har(har_file)
    requests = []
    responses = []
    cookies = []
    privacy_word_list = init_privacy_word_list()
    contact_word_list = init_contact_word_list()
    legal_word_list = init_legal_word_list()
    adblock_rules = init_adblocker()
    cookie_duplicates = []


    for entry in har_data['entries']:
        # Requests
        request_id = request_id + 1
        req = {}
        req['scan_profile'] = measurement_id
        req['request_id'] = request_id
        req['url'] = entry['request']['url']
        req['method'] = entry['request']['method']
        req['channelname'] = entry['request'].get('channelname', None)
        req['channelid'] = entry['request'].get('channelid', None)
        req['httpVersion'] = entry['request'].get('httpVersion', None)
        req['headers'] = urllib.parse.unquote(str(entry['request']['headers']))

        try:
            req['status'] = entry['response']['status']
        except:
            req['status'] = None

        try:
            req['postData'] = entry['request']['postData']['text']
        except:
            req['postData'] = None

        try:
            req['referrer'] = entry['request']['headers']['Referer']
        except:
            req['referrer'] = None

        try:
            req['resource_type'] = entry['request']['headers']['Accept']
        except:
            req['resource_type'] = None

        req['etld'] = tldextract.extract(req['url']).registered_domain
        req['time_stamp'] = entry['startedDateTime']
        req['ip_address'] = entry.get('serverIPAddress', None)
        req['cookies'] = urllib.parse.unquote(str(entry['request'].get('cookies', None)))
        req['queryString'] = urllib.parse.unquote(str(entry['request'].get('queryString', None)))
        req['headersSize'] = entry['request'].get('headersSize', None)
        req['bodySize'] = entry['request'].get('bodySize', None)
        req['is_known_tracker'] = is_known_blocker(req['url'], adblock_rules)

        requests.append(req)

        # Responses
        resp = {}
        resp['scan_profile'] = measurement_id
        resp['request_id'] = request_id
        resp['channel'] = entry['response']['channelname']
        resp['channel_id'] = entry['response']['channelid']
        resp['url'] = entry['request']['url']
        response = urllib.parse.unquote(entry['response']['content']['text'])
        resp['response'] = response
        resp['headers'] = urllib.parse.unquote(str(entry['response']['headers']))
        resp['size'] = entry['response']['bodySize']

        fmt = '%Y-%m-%dT%H:%M:%S.%f%z'
        tstamp1 = datetime.strptime(entry['startedDateTime'], fmt)
        tstamp1 = tstamp1 + timedelta(milliseconds=entry['time'])
        resp['time_stamp'] = tstamp1.isoformat(sep='T', timespec='auto')
        resp['type'] = entry['response']['content']['mimeType']
        resp['status'] = entry['response']['status']
        resp['cookies'] = urllib.parse.unquote(str(entry['response']['cookies']))

        # "Guess if the response might include a Cookie Banner or privacy policy
        resp['pp_candidate'] = is_privacy_policy(response.lower(), resp['headers'], resp['url'])
        resp['pp_candidate_strict'] = is_privacy_policy2(response.lower(), resp['headers'], resp['url'])
        resp['cb_candidate'] = is_cookie_banner(response.lower(), resp['headers'], resp['url'])
        resp['cb_candidate2'] = is_cookie_banner2(response.lower(), resp['headers'], resp['url'], privacy_word_list)
        resp['legal_candidate'] = is_cookie_banner2(response.lower(), resp['headers'], resp['url'], legal_word_list)
        resp['contact_candidate'] = is_cookie_banner2(response.lower(), resp['headers'], resp['url'], contact_word_list)

        try:
            resp['ip_address'] = entry['serverIPAddress']
        except:
            resp['ip_address'] = None
        # print(resp)
        responses.append(resp)

        # Cookies
        for item in entry['request']['cookies']:
            c = {}
            c['request_id'] = ""
            c['scan_profile'] = measurement_id
            c['origin'] = tldextract.extract(req['url']).subdomain + '.' + tldextract.extract(
                req['url']).domain + '.' + tldextract.extract(req['url']).suffix
            c['name'] = item['name']
            c['value'] = urllib.parse.unquote(item['value'])
            c['http_only'] = int(item['httpOnly'])
            c['secure'] = int(item['secure'])
            c['path'] = item.get("path", '/')
            c['expires'] = item.get("expires", 'session')
            # print(c['expires'], item)
            c['samesite1'] = item.get("sameSite", '')
            c['sent_in_request'] = True
            c['duplicate'] = False

            if c in cookie_duplicates:
                c['duplicate'] = True
            else:
                cookie_duplicates.append(dict(c))

            c['request_id'] = request_id

            cookies.append(c)

        for item in entry['response']['cookies']:
            c = {}
            c['request_id'] = request_id
            c['scan_profile'] = measurement_id
            c['origin'] = tldextract.extract(req['url']).subdomain + '.' + tldextract.extract(
                req['url']).domain + '.' + tldextract.extract(req['url']).suffix
            c['name'] = item['name']
            c['value'] = urllib.parse.unquote(item['value'])
            c['http_only'] = int(item['httpOnly'])
            c['secure'] = int(item['secure'])
            c['path'] = item.get("path", '/')
            c['expires'] = item.get("Expires", 'session')
            c['samesite1'] = item.get("sameSite", '')
            c['sent_in_request'] = False
            c['duplicate'] = False
            cookies.append(c)

    return request_id, requests, responses, cookies


def is_privacy_policy2(content, resp_header, url):
    for keyword in KEYWORDS_PRIVACY_POLICY:
        if keyword in url.lower():
            return True
    return False


def is_privacy_policy(content, resp_header, url):
    # Find a keyword in the URL or in the content of the response
    content_type = None
    try:
        resp_header = json.loads(resp_header.replace("\'", "\"").replace("\"\"", "\""))
        for header in resp_header:
            if header['name'] == 'Content-Type':
                content_type = header['value']
    except json.decoder.JSONDecodeError:
        try:
            resp_header = resp_header.replace('\"', "'").replace("{\'", "{\"").replace("\'}", "\"}") \
                .replace("\', \'", "\", \"").replace("\': \'", "\": \"")
            resp_header = json.loads(resp_header)
            for header in resp_header:
                if header['name'] == 'Content-Type' or header['name'] == 'content-type':
                    content_type = header['value']
        except json.decoder.JSONDecodeError:
            # print("resp_header", resp_header)
            content_type = "Could not parse header"

    # Find keywords
    url = url.split("?")[0]
    for keyword in KEYWORDS_PRIVACY_POLICY:
        if content_type is not None:
            if 'css' not in content_type.lower() and 'script' not in content_type and 'image' not in content_type and keyword in content.lower():
                return True
        # elif keyword in url.lower():
        #     return True

        if content_type is not None and 'css' not in content_type.lower() and 'script' not in content_type \
                and keyword in content:
            return True

    return False


def init_privacy_word_list():
    list_of_privacy_words = set()

    with open(os.path.join('resources', 'privacy-words.txt'), encoding="utf-8") as f:
        lines = f.readlines()
    for line in lines:
        list_of_privacy_words.add(ast.literal_eval(line.replace(',', '')).strip())

    return list_of_privacy_words


def init_contact_word_list():
    list_of_contact_words = set()

    with open(os.path.join('resources', 'contact-words.txt'), encoding="utf-8") as f:
        lines = f.readlines()
    for line in lines:
        list_of_contact_words.add(ast.literal_eval(line.replace(',', '')).strip())

    return list_of_contact_words


def init_legal_word_list():
    list_of_legal_words = set()

    with open(os.path.join('resources', 'legal-words.txt'), encoding="utf-8") as f:
        lines = f.readlines()
    for line in lines:
        list_of_legal_words.add(ast.literal_eval(line.replace(',', '')).strip())

    return list_of_legal_words


def init_adblocker():
    """
    Initialize the adblock parser.

    :return: The parser object
    """
    with open(os.path.join('resources', 'easylist.txt'), encoding="utf-8") as f:
        raw_rules = f.read().splitlines()
    return AdblockRules(raw_rules)


def is_known_blocker(url, rules):
    """
    Tests if the given URL is present on the initialized block list.

    :param url: The URL to test
    :param rules: The parser object
    :return: TRUE if present on the list
    """
    return rules.should_block(url)


def is_cookie_banner2(content, resp_header, url, list_of_privacy_words):
    # Get the content type of the response
    content_type = None
    try:
        resp_header = json.loads(resp_header.replace("\'", "\"").replace("\"\"", "\""))
        for header in resp_header:
            if header['name'] == 'Content-Type':
                content_type = header['value']
    except json.decoder.JSONDecodeError:
        try:
            resp_header = resp_header.replace('\"', "'").replace("{\'", "{\"").replace("\'}", "\"}") \
                .replace("\', \'", "\", \"").replace("\': \'", "\": \"")
            resp_header = json.loads(resp_header)
            for header in resp_header:
                if header['name'] == 'Content-Type' or header['name'] == 'content-type':
                    content_type = header['value']
        except json.decoder.JSONDecodeError:
            # print("resp_header", resp_header)
            content_type = "Could not parse header"

    if content_type is not None and 'css' in content_type:
        return False

    for privacy_word in list_of_privacy_words:
        if privacy_word in url:
            return True

        if content_type is not None and 'html' in content_type:
            if privacy_word in content:
                return True
    return False


def is_cookie_banner(content, resp_header, url):
    # Find a keyword in the URL or in the content of the response
    # content_type = None
    # try:
    #     resp_header = json.loads(resp_header.replace("\'", "\"").replace("\"\"", "\""))
    #     for header in resp_header:
    #         if header['name'] == 'Content-Type':
    #             content_type = header['value']
    # except json.decoder.JSONDecodeError:
    #     resp_header = resp_header.replace('\"', "'").replace("{\'", "{\"").replace("\'}", "\"}") \
    #         .replace("\', \'", "\", \"").replace("\': \'", "\": \"")
    #     resp_header = json.loads(resp_header)
    #     for header in resp_header:
    #         if header['name'] == 'Content-Type' or header['name'] == 'content-type':
    #             content_type = header['value']

    if url == ":":
        pass

    for keyword in KEYWORDS_COOKIE_BANNERS:
        if keyword in content:
            return True

    return False

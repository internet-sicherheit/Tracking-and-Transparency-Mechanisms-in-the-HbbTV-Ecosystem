import os
import re
import time
import string
import datetime
import statistics
import traceback
import sys
import re
from collections import Counter
from pprint import pprint
from pathlib import Path
from urllib import request

from tinydb import TinyDB, Query
from tinydb import where as tinydb_where
from tinydb.storages import JSONStorage
from tinydb.middlewares import CachingMiddleware

from chardet.universaldetector import UniversalDetector
import chardet
from joblib import Parallel, delayed, load
import psutil
import stopit
from tqdm import tqdm
from boilerpipe.extract import Extractor
from readabilipy import simple_json_from_html_string
from bs4 import BeautifulSoup
from html_sanitizer import Sanitizer
from markdownify import markdownify as md
from tqdm import tqdm, trange
import pandas as pd

from publicsuffix2 import PublicSuffixList
from publicsuffix2 import get_public_suffix
import tldextract
from tld import get_tld, get_fld
from urllib.parse import unquote, urlparse

import pycld2 as cld2
import cld3
from langdetect import detect, detect_langs, DetectorFactory, lang_detect_exception
from guess_language import guess_language
import fasttext
import textacy
from textacy import preprocessing as textacy_preprocessing
import ftfy

import ndjson
import ujson
import json

import yake
import spacy
import pke
from tqdm import tqdm
from multi_rake import Rake

from difflib import SequenceMatcher

import hashlib
import simhash

from pandas.core.common import flatten
import fitz

from google.cloud import bigquery


sanitizer = Sanitizer()
os.environ["TOKENIZERS_PARALLELISM"] = "false"


spacy_languages = {
    "de": "de_core_news_lg",
    "el": "el_core_news_lg",
    "en": "en_core_web_lg",
    "es": "es_core_news_lg",
    "fr": "fr_core_news_lg",
    "it": "it_core_news_lg",
    "nl": "nl_core_news_lg",
    "pt": "pt_core_news_lg",
    "xx": "xx_ent_wiki_sm",
    "nb": "nb_core_news_lg",
    "lt": "lt_core_news_lg",
    "zh": "zh_core_web_lg",
    "da": "da_core_news_lg",
    "ja": "ja_core_news_lg",
    "pl": "pl_core_news_lg",
    "ro": "ro_core_news_lg",
}

dict_of_umlaute_errors = {'Ã¼':'ü',
                            'Ã¤':'ä',
                            'Ã¶':'ö',
                            'Ã–':'Ö',
                            'ÃŸ':'ß',
                            'Ã ':'à',
                            'Ã¡':'á',
                            'Ã¢':'â',
                            'Ã£':'ã',
                            'Ã¹':'ù',
                            'Ãº':'ú',
                            'Ã»':'û',
                            'Ã™':'Ù',
                            'Ãš':'Ú',
                            'Ã›':'Û',
                            'Ãœ':'Ü',
                            'Ã²':'ò',
                            'Ã³':'ó',
                            'Ã´':'ô',
                            'Ã¨':'è',
                            'Ã©':'é',
                            'Ãª':'ê',
                            'Ã«':'ë',
                            'Ã€':'À',
                            'Ã':'Á',
                            'Ã‚':'Â',
                            'Ãƒ':'Ã',
                            'Ã„':'Ä',
                            'Ã…':'Å',
                            'Ã‡':'Ç',
                            'Ãˆ':'È',
                            'Ã‰':'É',
                            'ÃŠ':'Ê',
                            'Ã‹':'Ë',
                            'ÃŒ':'Ì',
                            'Ã':'Í',
                            'ÃŽ':'Î',
                            'Ã':'Ï',
                            'Ã‘':'Ñ',
                            'Ã’':'Ò',
                            'Ã“':'Ó',
                            'Ã”':'Ô',
                            'Ã•':'Õ',
                            'Ã˜':'Ø',
                            'Ã¥':'å',
                            'Ã¦':'æ',
                            'Ã§':'ç',
                            'Ã¬':'ì',
                            'Ã­':'í',
                            'Ã®':'î',
                            'Ã¯':'ï',
                            'Ã°':'ð',
                            'Ã±':'ñ',
                            'Ãµ':'õ',
                            'Ã¸':'ø',
                            'Ã½':'ý',
                            'Ã¿':'ÿ',
                            'â‚¬':'€'}

dict_of_umlaute_errors = {**dict_of_umlaute_errors,
                          **{key.lower(): value for key, value in dict_of_umlaute_errors.items()}}

def exec_select_query(query):
    """
    Executes the given SQL query using the static Google authentication credentials.

    :param query: The SQL query
    :return: A (pandas) dataframe that contains the results
    """
    # Initialize the Google BigQuery client. The authentication token should be placed in the working directory in the
    # following path: /resources/google.json
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "code", "resources", "google.json")
    client = bigquery.Client()

    # Execute the query and retrieve result data (as pandas dataframe)
    result_df = client.query(query).to_dataframe()
    print(result_df)
    return result_df


if len(sys.argv) != 2:
    print("Please give the folder containing the raw file as input. For example: python hbbtv_privacy_policy_preprocessing.py ./data/Measurement_XXXXXXX", flush=True)
    sys.exit()
else:
    data_dir = sys.argv[1]
    crawl = data_dir.split("/")[-1].lstrip("Measurement_")
    print("Working on crawl:", crawl, flush=True)

def process_har(har_file):
    har_parser = HarParser.from_file(har_file)
    har_data = har_parser.har_data
    har_data['entries'] = [entry for entry in har_data['entries']]
    return har_data

def load_data_of_text_policies(db, language=None):
    policies_table = db.table("policies")
    list_of_policies_dicts = policies_table.all()
    print("list_of_policies_dicts: {}".format(len(list_of_policies_dicts)), flush=True)

    if language:
        language_table = db.table("policies_language")
        list_of_language_dicts = language_table.search(
            tinydb_where("determined_language") == language
        )
        print("list_of_language_dicts: {}".format(len(list_of_language_dicts)), flush=True)

        list_of_language_IDs = [
            language_dict["text_id"] for language_dict in list_of_language_dicts
        ]
        print("list_of_languageIDs: {}".format(len(list_of_language_IDs)), flush=True)

        list_of_policies_dicts = [
            policy_dict
            for policy_dict in list_of_policies_dicts
            if policy_dict["text_id"] in list_of_language_IDs
        ]

    list_of_texts = [
        policy_dict["text"] for policy_dict in list_of_policies_dicts
    ]
    list_of_IDs = [policy_dict["text_id"] for policy_dict in list_of_policies_dicts]
    print("Number of loaded texts: {}".format(len(list_of_texts)), flush=True)
    del list_of_policies_dicts
    return list_of_texts, list_of_IDs


def fix_utf8_iso8859_errors(text):
    # source: https://sebastianviereck.de/mysql-php-umlaute-sonderzeichen-utf8-iso/
    for error, replacement in dict_of_umlaute_errors.items():
        text = text.replace(error, replacement)
    return text


def text_cleaner(text):

    text = textacy_preprocessing.normalize.bullet_points(text)
    text = textacy_preprocessing.normalize.unicode(text)
    text = ftfy.fix_text(text)
    text = fix_utf8_iso8859_errors(text)
    text = textacy_preprocessing.normalize.hyphenated_words(text)
    text = textacy_preprocessing.normalize.whitespace(text)
    text = textacy_preprocessing.replace.emails(text, "REPLACEDEMAIL")
    text = textacy_preprocessing.replace.urls(text, "REPLACEDurl")
    text = textacy_preprocessing.replace.phone_numbers(text, "REPLACEDPHONENUMBER")
    text = re.sub(
        " +",
        " ",
        "".join(x if x.isprintable() or x in string.whitespace else " " for x in text),
    )
    text = text.replace("\n", "\n\n")
    return text


def spacy_lemmatizer_with_whitespace(texts, language):
    lemmatized_docs = []
    nlp = spacy.load(spacy_languages[language], disable=["ner"])
    for text in tqdm(texts, desc="Spacy Lemmatization"):
        nlp.max_length = len(text)
        doc = nlp(text)
        lemmatized_docs.append(
            "".join([token.lemma_ + token.whitespace_ for token in doc])
        )
    return lemmatized_docs

def domain_cleaner(domain):
    domain = domain.lower()
    if domain.startswith("http://"):
        domain = domain.replace("http://", "", 1)
    elif domain.startswith("https://"):
        domain = domain.replace("https://", "", 1)
    else:
        domain = domain
    return domain

def get_policy_domain(url):
    url = url.lower() # lowercase everything
    url = "".join(url.splitlines()) # remove line breaks
    if url.startswith("http_"):
        url = url.replace("http_", "", 1)
    elif url.startswith("https_"):
        url = url.replace("https_", "", 1)
    if len(url.split("_")[0]) > 1:
        url = url.split("_")[0]
    if url.endswith("443"):
        url = url.rstrip("443")
    elif url.endswith("40018"):
        url = url.rstrip("40018")
    elif url.endswith("8090"):
        url = url.rstrip("8090")
    elif url.endswith("80"):
        url = url.rstrip("80")
    elif url.endswith("809"):
        url = url.rstrip("809")
    try:
        domain = get_fld(url, fail_silently=False, fix_protocol=True)
    except:
        domain = urlparse(url).netloc
    return domain


def stripprotocol(uri):
    noprotocolluri = ""
    if uri.find("https") == 0:
        noprotocolluri = uri[5:]
    else:
        noprotocolluri = uri[4:]
    return noprotocolluri


def filenamesplitter(filename):
    identifier = filename.split("_")
    host = identifier[0]
    uri = identifier[1]
    crawl = identifier[len(identifier) - 1]
    # if the uri contained an underscore this reconstructs the full uri
    if len(identifier) > 3:
        uri = ""
        for part in identifier:
            if identifier.index(part) not in [0, len(identifier) - 1]:
                uri += part + "_"
        uri = uri[:-1]
        uri = stripprotocol(uri)
    return host, uri, crawl


def text_extraction_module():

    def html_encoding_detection(raw_html):
        result = chardet.detect(raw_html.encode())
        return result["encoding"]

    def text_from_html_extraction_canola(raw_html):
        """Remove HTML/XML using Conola settings of Boilerpipe
        https://github.com/misja/python-boilerpipe
        """
        text = ""
        if raw_html:
            try:
                extractor = Extractor(extractor="CanolaExtractor", html=raw_html)
                text = str(extractor.getText())
            except:
                traceback.print_exc()
        return text

    def text_from_html_extraction_keepeverything(raw_html):
        """Remove HTML/XML using Conola settings of Boilerpipe
        https://github.com/misja/python-boilerpipe
        """

        text = ""
        if raw_html:
            try:
                extractor = Extractor(extractor="KeepEverythingExtractor", html=raw_html)
                text = str(extractor.getText())
            except:
                traceback.print_exc()
        return text

    def text_from_html_extraction_readability(raw_html):
        """
        remove HTML/XML with
        https://github.com/alan-turing-institute/ReadabiliPy
        """
        text = ""
        timeout = 10
        try:
            # throws memory erros for > 6.3MB files dispite updating node.js and Readability.js (2023.02.07)
            # Alternativly "while psutil.virtual_memory.percent < 50" but does not determine the amuont of memory this function/process is using. 
            with stopit.ThreadingTimeout(timeout) as context_manager:
                # https://theautomatic.net/2021/11/27/how-to-stop-long-running-code-in-python/
                result = simple_json_from_html_string(raw_html, use_readability=True)
                title = result["title"]
                if title is None:
                    title = ""
                plain_text = result["plain_text"][-1]["text"]
                if plain_text is None:
                    plain_text = ""
                text = title + "\n\n" + plain_text
            if context_manager.state == context_manager.TIMED_OUT:
                text = ""
            elif context_manager.state == context_manager.EXECUTED:
                pass
        except:
            text = ""
        return text

    def text_from_html_extraction_numwordsrules(raw_html):
        """Remove HTML/XML using NumWordsRules setting of Boilerpipe
        https://github.com/misja/python-boilerpipe
        """
        text = ""
        if raw_html:
            try:
                extractor = Extractor(extractor="NumWordsRulesExtractor", html=raw_html)
                text = str(extractor.getText())
            except:
                traceback.print_exc()
        return text

    def markdown_from_html_extraction_markdownify(raw_html):
        """Convert HTML/XML to Markdown format using
        https://github.com/matthewwithanm/python-markdownify
        """
        text = ""
        try:
            unwanted_tags = ["nav", "header", "footer"]
            soup = BeautifulSoup(raw_html, "lxml")
            _ = [tag.decompose() for tag in soup(unwanted_tags)]
            text = md(str(soup))
            # body = soup.find("body")
            # text = md(raw_html)

        except:
            traceback.print_exc()
            sys.exit()
        return text

    def text_from_pdf_extractor(pdf_path):
        text = ""
        try:
            with fitz.open(pdf_path) as doc:
                for page in doc:
                    text += page.get_text()
        except:
            traceback.print_exc()

        return text

    def makeSimhash(text):
        #https://github.com/seomoz/simhash-py/issues/47
        import ctypes
        list_of_tokens = re.split('\s+', re.sub(r'[^\w\s]', '', text.lower()))
        # A generator for ' '-joined strings of consecutive tokens
        shingles = (' '.join(tokens) for tokens in simhash.shingle(list_of_tokens, 4))
        # They need to be unsigned 64-bit ints
        return simhash.compute([ctypes.c_ulong(hash(shingle)).value for shingle in shingles])

    def makeSHA1hash(text):
        hashvalue = hashlib.sha1(text.encode()).hexdigest()
        return hashvalue

    def process_policies(item):
        temp_dict = None
        plain_text = ""
        plain_text_readability = ""
        plain_text_canola = ""
        markdown_text = ""
        try:
            raw_html = item["response"]
            raw_html = ftfy.fix_text(raw_html)
            html_encoding = html_encoding_detection(raw_html) # Encoding Detection
            # Debug: print("opened privacy policy", flush=True)
            raw_html = sanitizer.sanitize(raw_html)
            # Debug: print("sanitized privacy policy", flush=True)
            plain_text = fix_utf8_iso8859_errors(text_from_html_extraction_numwordsrules(raw_html))
            # Debug: print("numworldsrules privacy policy", flush=True)
            plain_text_readability = fix_utf8_iso8859_errors(text_from_html_extraction_readability(raw_html))
            # Debug: print("readability privacy policy", flush=True)
            plain_text_canola = fix_utf8_iso8859_errors(text_from_html_extraction_canola(raw_html))
            # Debug: print("canola privacy policy", flush=True)
            markdown_text = fix_utf8_iso8859_errors(markdown_from_html_extraction_markdownify(raw_html))
            # Debug: print("markdown privacy policy", flush=True)
            temp_dict = {
                "text_id": str(i) + "_" + crawl,
                "request_id": item["request_id"],
                "channel_id": item["channel_id"],
                "url": item["url"],
                "policy_domain": get_fld(unquote(item["url"].lstrip("%3A%2F%2F")), fail_silently=True, fix_protocol=True),
                "html_encoding": html_encoding,
                "crawl": crawl,
                "size": item["size"],
                "type": item["type"],
                "status": item["status"],
                "ip_address": item["ip_address"],
                "channel": item["channel"],
                "time_stamp": str(item["time_stamp"]),
                "scan_profile": item["scan_profile"],
                "cb_candidate": item["cb_candidate"],
                "cb_candidate2": item["cb_candidate2"],
                "pp_candidate": item["pp_candidate"],
                "pp_candidate_strict": item["pp_candidate_strict"],
                "is_first_party": item["is_first_party"],
                "is_third_party": item["is_third_party"],
                "is_iptv": item["is_iptv"],
                "legal_candidate": item["legal_candidate"],
                "contact_candidate": item["contact_candidate"],
                "sha1": makeSHA1hash(plain_text),
                "simhash": makeSimhash(plain_text),
                "text": plain_text,
                "text_canola":plain_text_canola,
                "text_readability": plain_text_readability,
                "text_markdown": markdown_text
            }
        except:
            print(item["url"], flush=True)
            traceback.print_exc()
            temp_dict = None
        return temp_dict


    # entry point

    storage = CachingMiddleware(JSONStorage)
    storage.WRITE_CACHE_SIZE = 25
    Path(data_dir).mkdir(parents=True, exist_ok=True)
    db = TinyDB(
        os.path.join(data_dir, "hbbtv_policies_database_" + crawl + ".json"),
        storage=storage,
        ensure_ascii=False,
        encoding="utf-8"
    )
    table_policies = db.table("policies")

    # read or fetch data from database

    if os.path.isfile("./data/Measurement_" + crawl + "/responses.json"):
        print("'responses.json' file already exists. Loading data ...")
        # df_responses = pd.read_json("./data/Measurement_" + crawl + "/responses.json", lines=True, orient="records", encoding="UTF-8")
        # df_responses["time_stamp"] = pd.to_datetime(df_responses.time_stamp, unit="ms")
        # print(df_responses.info())
        # print(df_responses.head())
    else:
        print("'responses.json' file does not exist. Querying data from BigQuery database ...")
        responses_query = """SELECT * FROM `hbbtv-research.hbbtv.responses`;"""
        df_responses = exec_select_query(responses_query)
        Path("./data/Measurement_" + crawl + "/").mkdir(parents=True, exist_ok=True)
        print("Retreived responsed from BigQuery database. Storing data to 'responses.json'")
        df_responses.to_json("./data/Measurement_" + crawl + "/responses.json", lines=True, orient="records", force_ascii=False, date_format="iso")
        del df_responses
        # df_responses["time_stamp"] = pd.to_datetime(df_responses.time_stamp, unit="ms")        
        # print(df_responses.info())
        # print(df_responses.head())

    i = 1
    with open("./data/Measurement_" + crawl + "/responses.json", encoding="UTF-8", mode="r") as f:
        reader = ndjson.reader(f)
        for entry in tqdm(reader, desc="response item"):
            if ((entry["cb_candidate"] is True) or (entry["cb_candidate2"] is True) or (entry["pp_candidate"] is True) or (entry["pp_candidate_strict"] is True) or (entry["legal_candidate"] is True) or (entry["contact_candidate"] is True)):
                try:
                    policy = Query()
                    result = table_policies.search((policy.channel == entry["channel"]) & (policy.url == entry["url"]) & (policy.request_id == crawl) & (policy.request_id == entry["request_id"]) & (policy.channel_id == entry["channel_id"]))
                    if len(result) == 0:
                        print(f'Processing privacy policy of {entry["channel"]} with request_id: {entry["request_id"]}', flush=True)
                        temp_dict = process_policies(entry)
                        if temp_dict is not None:
                            print(f'Inserting privacy policy {temp_dict["channel"]} retrieved from {temp_dict["policy_domain"]}', flush=True)
                            table_policies.upsert(temp_dict, tinydb_where("text_id") == str(i) + "_" + crawl)
                            i = i+1
                        else:
                            continue
                    elif len(result) == 1:
                        print("This already exists in the database:", entry["url"], flush=True)
                        i = i+1
                        continue
                except:
                    traceback.print_exc()
                    continue
    db.close()
    print("End time of text extraction: ", str(datetime.datetime.now()), flush=True)


def language_detection_module():
    """Performs majority voting on the detected languages by the libraries"""

    def segment_multilingual_policies(vectors, text):
        """segments privacy policies by language if desired
            by using the output vectors of CLD2
        """
        list_of_segments = []
        text_as_bytes = text.encode("utf-8")
        for vector in vectors:
            start = vector[0]
            end = start + vector[1]
            segment = text_as_bytes[start:end].decode("utf-8")
            list_of_segments.append(segment)
        return list_of_segments

    def language_detection(text):

        ## prepare components ##
        DetectorFactory.seed = 0

        fasttext_model = fasttext.load_model("./code/resources/lid.176.bin")

        word_re = re.compile(
            r"\w+", re.IGNORECASE | re.DOTALL | re.UNICODE | re.MULTILINE
        )

        # Just keep the words
        raw_text = textacy_preprocessing.replace.urls(
            textacy_preprocessing.replace.emails(text, ""), ""
        )
        raw_text = textacy_preprocessing.replace.phone_numbers(raw_text, "")
        raw_text = word_re.findall(raw_text)

        if len(raw_text) > 10:
            raw_text = " ".join(raw_text)
            dict_of_detected_languages = {}
            dict_of_detection_probabilies = {}

            # 1. https://github.com/Mimino666/langdetect
            DetectorFactory.seed = 0
            try:
                dict_of_detected_languages["langdetect"] = detect(raw_text).lower()
                dict_of_detection_probabilies["langdetect_probablities"] = [
                    (item.lang, item.prob) for item in detect_langs(raw_text)
                ]
            except lang_detect_exception.LangDetectException:
                traceback.print_exc()
                dict_of_detected_languages["langdetect"] = "un"
                dict_of_detection_probabilies["langdetect_probablities"] = []

            # 2. https://github.com/aboSamoor/pycld2
            try:
                isReliable, _, details, vectors = cld2.detect(
                    raw_text, returnVectors=True
                )
                if isReliable:
                    # utf-8 bytes issue with meaningless "un"
                    dict_of_detected_languages["pycld2"] = [
                        detail[1].lower() for detail in details if detail[2] != 0
                    ]
                    dict_of_detection_probabilies["pycld2_vectors"] = list(vectors)
                else:
                    dict_of_detected_languages["pycld2"] = ["un"]
                    dict_of_detection_probabilies["pycld2_vectors"] = ()
            except:
                traceback.print_exc()
                dict_of_detected_languages["pycld2"] = ["un"]
                dict_of_detection_probabilies["pycld2_vectors"] = ()

            # 3. https://github.com/saffsd/langid.py
            try:
                from langid.langid import LanguageIdentifier, model

                langid_identifier = LanguageIdentifier.from_modelstring(
                    model, norm_probs=True
                )
                langid_tuple = langid_identifier.classify(raw_text)
                dict_of_detected_languages["langid"] = langid_tuple[0].lower()
                dict_of_detection_probabilies["langid_probability"] = langid_tuple
            except:
                traceback.print_exc()
                dict_of_detected_languages["langid"] = "un"
                dict_of_detection_probabilies["langid_probability"] = ()

            # 4. https://bitbucket.org/spirit/guess_language/
            try:
                dict_of_detected_languages["guess_language"] = guess_language(
                    raw_text
                ).lower()
            except:
                traceback.print_exc()
                dict_of_detected_languages["guess_language"] = "un"

            # 5. https://github.com/facebookresearch/fasttext/tree/master/python
            # https://fasttext.cc/docs/en/language-identification.html
            try:
                dict_of_detected_languages["fasttext"] = (
                    fasttext_model.predict(raw_text)[0][0]
                    .replace("__label__", "")
                    .lower()
                )
                dict_of_detection_probabilies[
                    "fasttext_probability"
                ] = fasttext_model.predict(raw_text)[1]
            except:
                traceback.print_exc()
                dict_of_detected_languages["fasttext"] = "un"
                dict_of_detection_probabilies["fasttext_probability"] = 0

            # 6. https://github.com/chartbeat-labs/textacy/blob/master/textacy/lang_utils.py
            try:
                dict_of_detected_languages[
                    "textacy"
                ] = textacy.identify_lang(raw_text).lower()
            except:
                #traceback.print_exc()
                dict_of_detected_languages["textacy"] = "un"

            # 7. https://github.com/bsolomon1124/pycld3
            try:
                tuple_of_detected_language = cld3.get_language(raw_text)
                isReliable = tuple_of_detected_language[2]
                if isReliable:  # is_reliable
                    dict_of_detected_languages["cld3"] = tuple_of_detected_language[
                        0
                    ].lower()
                    dict_of_detection_probabilies[
                        "cld3_probabilities"
                    ] = cld3.get_frequent_languages(raw_text, num_langs=10)
                else:
                    dict_of_detected_languages["cld3"] = "un"
                    dict_of_detection_probabilies[
                        "cld3_probabilities"
                    ] = cld3.get_frequent_languages(raw_text, num_langs=10)
            except:
                traceback.print_exc()
                dict_of_detected_languages["cld3"] = "un"
                dict_of_detection_probabilies["cld3_probabilities"] = []

            list_of_all_detected_languages = list(
                flatten(dict_of_detected_languages.values())
            )
            list_of_all_detected_languages = [
                v if not v.startswith("zh") else "zh"
                for v in list_of_all_detected_languages
            ]
            list_of_all_detected_languages = [
                v
                if (v not in ("unknown", "UNKNOWN", "UNKNOWN_LANGUAGE"))
                else "un"
                for v in list_of_all_detected_languages
            ]
            try:
                determined_language = statistics.mode(list_of_all_detected_languages)
            except statistics.StatisticsError:
                determined_language = "no-majority-achieved"

            # handling multilingual cases
            if (
                len(dict_of_detected_languages["pycld2"]) > 1
                or len(dict_of_detection_probabilies["cld3_probabilities"]) > 1
            ):
                multilingual = True
            else:
                multilingual = False

            # possibility for superflous strings as described in the paper
            if len(set(list_of_all_detected_languages))==1 and multilingual is True:
                recheck = True # Mark to check whether CanolaExtractor or Readability.js could provide purer plain text
            else:
                recheck = False

        else:
            determined_language = "too-short-text"
            dict_of_detected_languages = {}
            dict_of_detection_probabilies = {}
            multilingual = False
            recheck = False

        return (
            determined_language,
            dict_of_detected_languages,
            dict_of_detection_probabilies,
            multilingual,
            recheck
        )

    print("Start time: ", str(datetime.datetime.now()), flush=True)
    storage = CachingMiddleware(JSONStorage)
    storage.WRITE_CACHE_SIZE = 25
    db = TinyDB(
        os.path.join(data_dir, "hbbtv_policies_database_" + crawl + ".json"),
        storage=storage
    )
    language_table = db.table("policies_language")

    list_of_texts, list_of_ids = load_data_of_text_policies(db, language=None)

    if not os.path.exists("./code/resources/lid.176.bin"):
        Path("./code/resources/").mkdir(parents=True, exist_ok=True)

        print("Downloading language model of Fasttext ...", flush=True)
        request.urlretrieve("https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin", "code/resources/lid.176.bin")


    print("Start language detection", flush=True)
    res = Parallel(n_jobs=-1)(
        delayed(language_detection)(text) for text in tqdm(list_of_texts)
    )

    print("Finished language detection", flush=True)

    del list_of_texts
    list_of_determined_languages = [item[0] for item in res]  # list
    list_of_dicts_with_all_detected_languages = [item[1] for item in res]
    list_of_dicts_with_detection_probabilities = [item[2] for item in res]
    list_of_multilingual_booleans = [item[3] for item in res]
    list_of_rechecks_booleans = [item[4] for item in res]

    del res

    print(
        "Most common languages: {}".format(
            dict(Counter(list_of_determined_languages).most_common())), flush=True
    )

    print("Updating database with language metadata", flush=True)
    for id, language, multilingual, recheck in zip(tqdm(
        list_of_ids), list_of_determined_languages, list_of_multilingual_booleans, list_of_rechecks_booleans
    ):
        language_table.upsert(
            {
                "text_id": id,
                "determined_language": language,
                "multilingual": multilingual,
                "recheck": recheck
            }, tinydb_where("text_id") == id
        )

    print("Saving the probabilities of languages in ./logs/language_analysis", flush=True)
    df = pd.DataFrame(list_of_determined_languages, columns=["determined_language"])
    df.insert(loc=0, column="text_id", value=list_of_ids)
    df.insert(loc=1, column="multilingual", value=list_of_multilingual_booleans)
    df = pd.concat(
        [
            df,
            pd.DataFrame(list_of_dicts_with_all_detected_languages),
            pd.DataFrame(list_of_dicts_with_detection_probabilities),
        ],
        axis=1,
    )


    Path("./logs/language_analysis/").mkdir(parents=True, exist_ok=True)

    df.to_json(
        "./logs/language_analysis/language_detection_probabilities_" + crawl + ".json",
        orient="records",
    )

    db.close()

    print("End time of language detection: ", str(datetime.datetime.now()), flush=True)


def keyphrase_extraction_module():
    def multi_rake(text, language):
        # https://pypi.org/project/multi-rake/
        r = Rake(language_code=language)
        try:
            keyphrases = r.apply(text)
            keyphrases = [keyphrase for keyphrase, score in keyphrases]
            if len(keyphrases) > 20:
                list_of_keyphrases = keyphrases[:20]
            else:
                list_of_keyphrases = keyphrases
        except:
            tqdm.write(traceback.format_exc())
            list_of_keyphrases = []
        return list_of_keyphrases

    def yake_original(text, language):
        # https://pypi.org/project/yake/
        if language == "cs":
            language = "cz"
        kwextractor = yake.KeywordExtractor(lan=language)
        try:
            keyphrases = kwextractor.extract_keywords(text)
            keyphrases = [keyphrase for keyphrase, score in keyphrases]
            if len(keyphrases) > 20:
                list_of_keyphrases = keyphrases[:20]
            else:
                list_of_keyphrases = keyphrases
        except:
            tqdm.write(traceback.format_exc())
            list_of_keyphrases = []
        return list_of_keyphrases

    def pke_textrank(text, language):
        # https://github.com/boudinfl/pke
        extractor = pke.unsupervised.TextRank()
        try:
            extractor.load_document(input=text, language=language, normalization="none")
            extractor.candidate_selection()
            extractor.candidate_weighting()
            keyphrases = extractor.get_n_best(n=20)
            list_of_keyphrases = [keyphrase for keyphrase, score in keyphrases]
        except:
            tqdm.write(traceback.format_exc())
            list_of_keyphrases = []
        return list_of_keyphrases

    def pke_singlerank(text, language):
        extractor = pke.unsupervised.SingleRank()
        try:
            extractor.load_document(input=text, language=language, normalization="none")
            extractor.candidate_selection()
            extractor.candidate_weighting()
            keyphrases = extractor.get_n_best(n=20)
            list_of_keyphrases = [keyphrase for keyphrase, score in keyphrases]
        except:
            tqdm.write(traceback.format_exc())
            list_of_keyphrases = []
        return list_of_keyphrases

    def pke_topicrank(text, language):
        extractor = pke.unsupervised.TopicRank()
        try:
            extractor.load_document(input=text, language=language, normalization="none")
            extractor.candidate_selection()
            extractor.candidate_weighting()
            keyphrases = extractor.get_n_best(n=20)
            list_of_keyphrases = [keyphrase for keyphrase, score in keyphrases]
        except:
            tqdm.write(traceback.format_exc())
            list_of_keyphrases = []
        return list_of_keyphrases

    def pke_positionrank(text, language):
        extractor = pke.unsupervised.PositionRank()
        try:
            extractor.load_document(input=text, language=language, normalization="none")
            extractor.candidate_selection()
            extractor.candidate_weighting()
            keyphrases = extractor.get_n_best(n=20)
            list_of_keyphrases = [keyphrase for keyphrase, score in keyphrases]
        except:
            tqdm.write(traceback.format_exc())
            list_of_keyphrases = []
        return list_of_keyphrases

    def pke_multipartiterank(text, language):
        extractor = pke.unsupervised.MultipartiteRank()
        try:
            extractor.load_document(input=text, language=language, normalization="none")
            extractor.candidate_selection()
            extractor.candidate_weighting()
            keyphrases = extractor.get_n_best(n=20)
            list_of_keyphrases = [keyphrase for keyphrase, score in keyphrases]
        except:
            tqdm.write(traceback.format_exc())
            list_of_keyphrases = []
        return list_of_keyphrases

    def textacy_scake(text, language):
        try:
            doc = textacy.make_spacy_doc(text, lang=spacy_languages[language])
            keyphrases = textacy.extract.keyterms.scake(doc, normalize="lemma", topn=20)
            list_of_keyphrases = [keyphrase for keyphrase, score in keyphrases]
        except:
            tqdm.write(traceback.format_exc())
            list_of_keyphrases = []
        return list_of_keyphrases

    print("Start time: ", str(datetime.datetime.now()), flush=True)

    keyphrase_extractors = {
        "multi_rake": multi_rake,
        "yake_original": yake_original,
        "pke_textrank": pke_textrank,
        "pke_singlerank": pke_singlerank,
        "pke_topicrank": pke_topicrank,
        "pke_positionrank": pke_positionrank,
        "pke_multipartiterank": pke_multipartiterank,
        "textacy_scake": textacy_scake,
    }

    groups_of_algorithms = {
        "misc": ["multi_rake", "yake_original"],
        "pke": [
            "pke_textrank",
            "pke_singlerank",
            "pke_topicrank",
            "pke_positionrank",
            "pke_multipartiterank",
        ],
        "textacy": ["textacy_scake"],
    }

    list_of_languages = ["de", "en"]

    storage = CachingMiddleware(JSONStorage)
    storage.WRITE_CACHE_SIZE = 25
    db = TinyDB(
        os.path.join(data_dir, "hbbtv_policies_database_" + crawl + ".json"),
        storage=storage
    )

    lemmatized_table = db.table("policies_lemmatized")
    keyphrase_table = db.table("policies_keyphrases")

    for language in list_of_languages:

        list_of_texts, list_of_IDs = load_data_of_text_policies(db, language=language)

        if len(list_of_IDs) == 0 or len(list_of_texts) == 0:
            print("texts were not loaded properly!", flush=True)
            sys.exit(0)

        list_of_texts = Parallel(n_jobs=-1)(
            delayed(text_cleaner)(text)
            for text in tqdm(list_of_texts, desc="Cleaning texts")
        )
        list_of_lemmatized_texts = spacy_lemmatizer_with_whitespace(
            list_of_texts, language
        )
        for ID, lemmatized_text in zip(tqdm(list_of_IDs, desc="Save lemmatized text"), list_of_lemmatized_texts):
            lemmatized_table.upsert(
                {"text_id": ID, "language": language, "lemmatized_text": lemmatized_text},
                tinydb_where("text_id") == ID
            )

        ### MULTIPROCESSING VERSION ###
        list_of_keyphrase_dicts = []
        for ID in tqdm(list_of_IDs, desc="List of keyphrase dicts"):
            list_of_keyphrase_dicts.append({"text_id": ID, "keyphrases":set()})
        print("len(list_of_keyphrase_dicts):", len(list_of_keyphrase_dicts), flush=True)

        # Depending on whether the library does lemmatization itself or not, the appropriate list is passed to the function
        for name, extractor in keyphrase_extractors.items():
            if name in groups_of_algorithms["textacy"]:
                list_of_lists_of_keywords = Parallel(n_jobs=-1)(
                    delayed(extractor)(text, language)
                    for text in tqdm(list_of_texts, desc=name)
                )
            else:
                list_of_lists_of_keywords = Parallel(n_jobs=-1)(
                    delayed(extractor)(text, language)
                    for text in tqdm(list_of_lemmatized_texts, desc=name)
                )

            for i, ID in enumerate(list_of_IDs):
                if list_of_keyphrase_dicts[i]["text_id"] == ID:
                    list_of_keyphrase_dicts[i]["keyphrases"].update(set(list_of_lists_of_keywords[i]))
                    # list_of_keyphrase_dicts[i] = {
                    #     **list_of_keyphrase_dicts[i],
                    #     **{name: list_of_lists_of_keywords[i]},
                    # }

        # tinydb does not like sets as they are not serialiseable
        for keyphrase_dict in list_of_keyphrase_dicts:
            keyphrase_dict["keyphrases"] = list(keyphrase_dict["keyphrases"])

        print("Saving extracted keyphrases", flush=True)
        assert len(list_of_IDs) == len(list_of_keyphrase_dicts)
        for ID, keyphrase_dict in zip(tqdm(list_of_IDs), list_of_keyphrase_dicts):
            keyphrase_table.upsert(keyphrase_dict, tinydb_where("text_id") == ID)

        ### SINGLE PROCESSING VERSION IF MULTIPROCESSING DOES NOT WORK ###
        # for ID, lemmatized_text, text in zip(tqdm(list_of_IDs, desc="keyphrase extraction"), list_of_lemmatized_texts, list_of_texts):
        #     keyphrase_dict = {"text_id": ID, "keyphrases":set()}
        #     for name, extractor in keyphrase_extractors.items():
        #         if name in groups_of_algorithms["textacy"]:
        #             list_of_keywords = extractor(text, language)
        #         else:
        #             list_of_keywords = extractor(lemmatized_text, language)
        #         keyphrase_dict["keyphrases"].update(set(list_of_keywords))
        #     keyphrase_dict["keyphrases"] = list(keyphrase_dict["keyphrases"])
        #     keyphrase_table.upsert(keyphrase_dict, tinydb_where("text_id") == ID)

    db.close()

    print("End time of keyphrase extraction: ", str(datetime.datetime.now()), flush=True)


def policy_detection_module():

    def load_keyphrases(db, language):
        list_of_lists_of_keyphrases = []
        print("Loading keyphrases in {}".format(language), flush=True)

        keyphrase_table = db.table("policies_keyphrases")
        list_of_keyphrase_dicts = keyphrase_table.all()

        language_table = db.table("policies_language")
        list_of_language_dicts = language_table.search(
            tinydb_where("determined_language") == language
        )
        print("list_of_language_dicts: {}".format(len(list_of_language_dicts)), flush=True)

        list_of_language_IDs = [
            language_dict["text_id"] for language_dict in list_of_language_dicts
        ]

        del list_of_language_dicts

        list_of_keyphrase_dicts = [
            keyphrases_dict
            for keyphrases_dict in list_of_keyphrase_dicts
            if keyphrases_dict["text_id"] in list_of_language_IDs
        ]

        list_of_text_ids = [
            keyphrase_dict["text_id"] for keyphrase_dict in list_of_keyphrase_dicts
        ]

        assert sorted(list_of_text_ids) == sorted(list_of_language_IDs)
        del list_of_language_IDs

        policies_table = db.table("policies")
        list_of_policies_dicts = policies_table.all()
        list_of_urls = [
            policy_dict["url"]
            for policy_dict in list_of_policies_dicts
            if policy_dict["text_id"] in list_of_text_ids
        ]

        del list_of_policies_dicts

        for keyphrase_dict in list_of_keyphrase_dicts:
            list_of_lists_of_keyphrases.append(keyphrase_dict["keyphrases"])

        print(len(list_of_lists_of_keyphrases), flush=True)
        list_of_lists_of_keyphrases = [
            list(set([keyphrase.lower() for keyphrase in list_of_keyphrases]))
            for list_of_keyphrases in list_of_lists_of_keyphrases
        ]
        return list_of_text_ids, list_of_urls, list_of_lists_of_keyphrases


    def keyphrase_analyzer(list_of_list_of_keyphrases):
        all_keywords = []
        list_of_dict_keyphrases = []
        number_of_policies = str(len(list_of_list_of_keyphrases))
        for list_of_keyphrases in list_of_list_of_keyphrases:
            all_keywords += list_of_keyphrases
            dict_of_keyphrases = dict(
                Counter(list_of_keyphrases)
            )
            list_of_dict_keyphrases.append(dict_of_keyphrases)
        print(number_of_policies + " policies:", Counter(all_keywords).most_common(50), flush=True)
        print("#unique keywords:", len(set(all_keywords)), flush=True)

        return list_of_dict_keyphrases

    def label_determination(
        language, list_of_dict_keyphrases, list_of_text_ids, list_of_urls
    ):

        print("Loading vectorizer and classifier", flush=True)
        vectorizer = load(
            "code/resources/trained_vectorizer_" + language + "_2023-01-22.pkl"
        )
        clf = load(
            "code/resources/VotingClassifier_soft_" + language + "_2023-01-22.pkl"
        )

        print("Vectorizer transformation", flush=True)
        X_unlabeled = vectorizer.transform(list_of_dict_keyphrases)

        print("Shape of unlabeled texts: {}".format(X_unlabeled.shape), flush=True)
        print("Predicting ...", flush=True)
        y_pred = clf.predict(X_unlabeled)
        y_pred_proba = clf.predict_proba(X_unlabeled)

        print("0: Miscellanous, 1: Privacy policy", flush=True)
        print(Counter(y_pred), flush=True)
        print("Converting the results to data frame", flush=True)
        df = pd.DataFrame()
        df["text_id"] = list_of_text_ids
        df["url"] = list_of_urls
        df["language"] = language
        df["predicted_label"] = y_pred
        df_proba = pd.DataFrame(
            y_pred_proba, columns=["probability_0", "probability_1"]
        )
        df = pd.concat([df, df_proba], axis=1)

        print("Saving the data frame", flush=True)
        Path("results/classification/").mkdir(parents=True, exist_ok=True)
        df.to_csv("results/classification/classification_" + language + "_" + crawl + ".csv")
        return df

    print("Start time of policy detection: ", str(datetime.datetime.now()), flush=True)

    storage = CachingMiddleware(JSONStorage)
    storage.WRITE_CACHE_SIZE = 25
    db = TinyDB(
        os.path.join(data_dir, "hbbtv_policies_database_" + crawl + ".json"),
        storage=storage
    )
    label_table = db.table("policies_labels")

    list_of_languages = ["de", "en"]

    for language in tqdm(list_of_languages, unit="language"):
        print(f"Loading keyphrases of {language}", flush=True)
        (
            list_of_text_ids,
            list_of_urls,
            list_of_lists_of_keyphrases,
        ) = load_keyphrases(db, language)
        list_of_dict_keyphrases = keyphrase_analyzer(list_of_lists_of_keyphrases)

        if len(list_of_text_ids) > 0:
            df_labels = label_determination(
                language, list_of_dict_keyphrases, list_of_text_ids, list_of_urls
            )
        else:
            print("No data for language", language, flush=True)
        list_of_dicts_with_label = df_labels.to_dict("records")
        del df_labels
        for dict_with_label in tqdm(list_of_dicts_with_label, unit="text", desc="Writing labels to database"):
            label_table.upsert(dict_with_label, tinydb_where("text_id") == dict_with_label["text_id"])

    db.close()
    print("End time: ", str(datetime.datetime.now()), flush=True)

if __name__ == "__main__":
    text_extraction_module()
    language_detection_module()
    keyphrase_extraction_module()
    policy_detection_module()

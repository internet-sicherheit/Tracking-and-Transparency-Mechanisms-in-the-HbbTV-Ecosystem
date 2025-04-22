from haralyzer import HarParser
import os,json,sys

path = "{path}"

channel = {}

def getFiles(path):
    har_files = os.listdir(path)

    for file in har_files:
        cleanFile(path,file)

def process_har(har_file):
    har_parser = HarParser.from_file(har_file)
    har_data = har_parser.har_data
    har_data['entries'] = [entry for entry in har_data['entries']]

    return har_data

def cleanFile(path,filename):
    old_filename = filename
    new_filename = f"{old_filename[:len(old_filename)-4]}_cleaned_.har"
    output_path = f"{path}Cleaned/"

    if not os.path.isdir(output_path):
        os.mkdir(output_path)

    har_data = process_har(path+filename)
    #print(har_data.keys())

    for entry in har_data['entries']:
        mimeType = entry['response']['content']['mimeType']

        if mimeType == "application/octet-stream" or mimeType == "application/javascript" or mimeType == "text/javascript; charset=UTF-8" or mimeType == "text/css" or mimeType == "application/json":
            entry['response']['content']['text'] = "Cleaned"
            text = entry['response']['content']['text']
            #print(text)

    output_har = dict()
    output_har['log'] = har_data

    with open(output_path + new_filename, "w") as f:
        f.write(json.dumps(output_har))


getFiles(path)

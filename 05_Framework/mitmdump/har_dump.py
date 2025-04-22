"""
This inline script can be used to dump flows as HAR files.

example cmdline invocation:
mitmdump -s ./har_dump.py --set hardump=./dump.har

filename endwith '.zhar' will be compressed:
mitmdump -s ./har_dump.py --set hardump=./dump.zhar
"""

import base64
import json
import logging
import os
from datetime import datetime
from datetime import timezone

import zlib
import re

import mitmproxy
from mitmproxy import connection
from mitmproxy import ctx
from mitmproxy import version
from mitmproxy.net.http import cookies
from mitmproxy.utils import strutils

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


HAR: dict = {}

# A list of server seen till now is maintained so we can avoid
# using 'connect' time for entries that use an existing connection.
SERVERS_SEEN: set[connection.Server] = set()


def load(l):
    l.add_option(
        "hardump", str, "", "HAR dump path.",
    )


def configure(updated):
    HAR.update({
        "log": {
            "version": "1.2",
            "creator": {
                "name": "mitmproxy har_dump",
                "version": "0.1",
                "comment": "mitmproxy version %s" % version.MITMPROXY
            },
            "pages": [
                {
                    "pageTimings": {}
                }
            ],
            "entries": []
        }
    })
    # The `pages` attribute is needed for Firefox Dev Tools to load the HAR file.
    # An empty value works fine.


def flow_entry(flow: mitmproxy.http.HTTPFlow) -> dict:
    global HAR
    # -1 indicates that these values do not apply to current request
    ssl_time = -1
    connect_time = -1

    if flow.server_conn and flow.server_conn not in SERVERS_SEEN:
        connect_time = (flow.server_conn.timestamp_tcp_setup -
                        flow.server_conn.timestamp_start)

        if flow.server_conn.timestamp_tls_setup is not None:
            ssl_time = (flow.server_conn.timestamp_tls_setup -
                        flow.server_conn.timestamp_tcp_setup)

        SERVERS_SEEN.add(flow.server_conn)

    # Calculate raw timings from timestamps. DNS timings can not be calculated
    # for lack of a way to measure it. The same goes for HAR blocked.
    # mitmproxy will open a server connection as soon as it receives the host
    # and port from the client connection. So, the time spent waiting is actually
    # spent waiting between request.timestamp_end and response.timestamp_start
    # thus it correlates to HAR wait instead.
    timings_raw = {
        'send': flow.request.timestamp_end - flow.request.timestamp_start,
        'receive': flow.response.timestamp_end - flow.response.timestamp_start,
        'wait': flow.response.timestamp_start - flow.request.timestamp_end,
        'connect': connect_time,
        'ssl': ssl_time,
    }

    # HAR timings are integers in ms, so we re-encode the raw timings to that format.
    timings = {
        k: int(1000 * v) if v != -1 else -1
        for k, v in timings_raw.items()
    }

    # full_time is the sum of all timings.
    # Timings set to -1 will be ignored as per spec.
    full_time = sum(v for v in timings.values() if v > -1)

    started_date_time = datetime.fromtimestamp(flow.request.timestamp_start, timezone.utc).isoformat()

    # Response body size and encoding
    response_body_size = len(flow.response.raw_content) if flow.response.raw_content else 0
    response_body_decoded_size = len(flow.response.content) if flow.response.content else 0
    response_body_compression = response_body_decoded_size - response_body_size

    entry = {
        "startedDateTime": started_date_time,
        "time": full_time,
        "request": {
            "channelname": getChannelName(),
            "channelid": getChannelId(),
            "method": flow.request.method,
            "url": flow.request.pretty_url,
            "httpVersion": flow.request.http_version,
            "cookies": format_request_cookies(flow.request.cookies.fields),
            "headers": name_value(flow.request.headers),
            "queryString": name_value(flow.request.query or {}),
            "headersSize": len(str(flow.request.headers)),
            "bodySize": len(flow.request.content),
        },
        "response": {
            "channelname": getChannelName(),
            "channelid": getChannelId(),
            "status": flow.response.status_code,
            "statusText": flow.response.reason,
            "httpVersion": flow.response.http_version,
            "cookies": format_response_cookies(flow.response.cookies.fields),
            "headers": name_value(flow.response.headers),
            "content": {
                "size": response_body_size,
                "compression": response_body_compression,
                "mimeType": flow.response.headers.get('Content-Type', '') #need to be removed!
            },
            "redirectURL": flow.response.headers.get('Location', ''),
            "headersSize": len(str(flow.response.headers)),
            "bodySize": response_body_size,
        },
        "cache": {},
        "timings": timings,
    }

    # Store binary data as base64
    content_type = flow.response.headers.get('Content-Type', '')

    is_text = re.search("^text.*", content_type)
    is_application = re.search("^application.*", content_type)
    if is_text or is_application:
        if strutils.is_mostly_bin(flow.response.content):
            entry["response"]["content"]["text"] = base64.b64encode(flow.response.content).decode()
            entry["response"]["content"]["encoding"] = "base64"
        else:
            entry["response"]["content"]["text"] = flow.response.get_text(strict=False)
    else:
        entry["response"]["content"]["text"] = "'" + content_type + "' is not being dumped."

    if flow.request.method in ["POST", "PUT", "PATCH"]:
        params = [
            {"name": a, "value": b}
            for a, b in flow.request.urlencoded_form.items(multi=True)
        ]
        entry["request"]["postData"] = {
            "mimeType": flow.request.headers.get("Content-Type", ""),
            "text": flow.request.get_text(strict=False),
            "params": params
        }

    if flow.server_conn.connected:
        entry["serverIPAddress"] = str(flow.server_conn.peername[0])

    HAR["log"]["entries"].append(entry)

    if len(HAR["log"]["entries"]) >= 100:
        logger.info("Write entries to file")
        write_HAR_file()

        logger.info("Reset dict HAR")
        HAR.clear() # clear dict -> return an empty dict
        HAR.update({
            "log": {
                "version": "1.2",
                "creator": {
                    "name": "mitmproxy har_dump",
                    "version": "0.1",
                    "comment": "mitmproxy version %s" % version.MITMPROXY
                },
                "pages": [
                    {
                        "pageTimings": {}
                    }
                ],
                "entries": []
            }
        }) # update the meta information for HAR dict -> todo: cleaner update (maybe fix method on top ...)

    return entry

def getTimeStamp():
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

with open("{path}/unfug2.txt", "r") as f:
    path = f.readline()

def write_HAR_file():
    """
    Write in HAR-file every 100 entries in header under ["log"]["entries"]
    """
    try:
        logger.info("Trying to write in har file")
        filename = ctx.options.hardump + "_" + str(getTimeStamp()) + "_dump.har" # file name with current datetime
        folder = path + filename


        json_dump: str = json.dumps(HAR, indent=2)
        raw: bytes = json_dump.encode()

        with open(folder, 'wb') as f:
            logger.info("Write in file %s", filename)
            f.write(raw)
        logger.info("Successfully wrote in har file (wrote %s bytes to file)" % len(json_dump))
    except Exception as e:
        logger.exception("Error while writing in har file: %s", e)
    finally:
        logger.info("leave write_HAR_file()")




def response(flow: mitmproxy.http.HTTPFlow):
    """
       Called when a server response has been received.
    """
    if flow.websocket is None:
        flow_entry(flow)


def websocket_end(flow: mitmproxy.http.HTTPFlow):
    entry = flow_entry(flow)

    websocket_messages = []

    for message in flow.websocket.messages:
        if message.is_text:
            data = message.text
        else:
            data = base64.b64encode(message.content).decode()
        websocket_message = {
            'type': 'send' if message.from_client else 'receive',
            'time': message.timestamp,
            'opcode': message.type.value,
            'data': data
        }
        websocket_messages.append(websocket_message)

    entry['_resourceType'] = 'websocket'
    entry['_webSocketMessages'] = websocket_messages


def done():
    """
        Called once on script shutdown, after any other events.
    """
    if ctx.options.hardump:
        json_dump: str = json.dumps(HAR, indent=2)

        if ctx.options.hardump == '-':
            print(json_dump)
        else:
            raw: bytes = json_dump.encode()
            if ctx.options.hardump.endswith('.zhar'):
                raw = zlib.compress(raw, 9)

            filename = ctx.options.hardump +"_"+ str(getTimeStamp()) + "_dump.har"
            #with open(os.path.expanduser(ctx.options.hardump), "wb") as f:
            #    f.write(raw)
            with open(path+filename, "wb") as f:
                f.write(raw)

            logging.info("HAR dump finished (wrote %s bytes to file)" % len(json_dump))


def format_cookies(cookie_list):
    rv = []

    for name, value, attrs in cookie_list:
        cookie_har = {
            "name": name,
            "value": value,
        }

        # HAR only needs some attributes
        for key in ["path", "domain", "comment"]:
            if key in attrs:
                cookie_har[key] = attrs[key]

        # These keys need to be boolean!
        for key in ["httpOnly", "secure", "sameSite"]:
            cookie_har[key] = bool(key in attrs)

        # Expiration time needs to be formatted
        expire_ts = cookies.get_expiration_ts(attrs)
        if expire_ts is not None:
            cookie_har["expires"] = datetime.fromtimestamp(expire_ts, timezone.utc).isoformat()

        rv.append(cookie_har)

    return rv


def format_request_cookies(fields):
    return format_cookies(cookies.group_cookies(fields))


def format_response_cookies(fields):
    return format_cookies((c[0], c[1][0], c[1][1]) for c in fields)


def name_value(obj):
    """
        Convert (key, value) pairs to HAR format.
    """
    return [{"name": k, "value": v} for k, v in obj.items()]


glob_path = "{path}/unfug.txt"

def getChannelName():
    with open(glob_path, 'r') as f:
        t = f.readline().split(';')[0]
    return t

def getChannelId():
    with open(glob_path, 'r') as f:
        t = f.readline().split(';')[1]
    return t

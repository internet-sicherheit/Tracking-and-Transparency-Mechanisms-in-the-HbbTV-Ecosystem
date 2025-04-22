import json
import psycopg2
import time
import random


class DBOps:
    def __init__(self): 
        str()

    def select_DEP(self, query, onerow=False):
        self.cr.execute(query)
        records = self.cr.fetchall()
        if onerow:
            for row in records:
                return row
        # self.cr.close()
        # self.connection.close()
        return records

    def exec_DEP(self, query):
        print(query)
        self.__init__()
        self.cr.execute(query)
        self.connection.commit()
        self.cr.close()
        self.connection.close()
        return self.cr.rowcount

    def close(self):
        self.cr.close()
        self.connection.close()

    def select(self, query, onerow=False):
        conString = "user=hbbtv_user  password=3}cP_sA3s_fgh!j!+@@ host=194.94.127.53 port=5432 dbname=hbbtv_research"
        try:
            connection = psycopg2.connect(conString)
            cr = connection.cursor()
        except (Exception, psycopg2.Error) as error:
            print("can't connect DB!", error)

        records = None
        try:
            with connection:
                with connection.cursor() as cr:
                    cr.execute(query)
                    records = cr.fetchall()
                    if onerow:
                        for row in records:
                            return row
                    cr.close()
                    connection.close()
                    # return records
        except:
            time.sleep(random.randint(0, 2))
            try:
                with connection:
                    with connection.cursor() as cr:
                        cr.execute(query)
                        records = cr.fetchall()
                        if onerow:
                            for row in records:
                                return row
                        cr.close()
                        connection.close()
                        # return records
            except:
                pass
        return records


    def exec(self, query):
        print(query)
        conString = "user=hbbtv_user  password=3}cP_sA3s_fgh!j!+@@ host=194.94.127.53 port=5432 dbname=hbbtv_research"
        try:
            connection = psycopg2.connect(conString)
            cr = connection.cursor()
        except (Exception, psycopg2.Error) as error:
            print("can't connect DB!", error)

        # print(query)
        try:
            with connection:
                with connection.cursor() as cr:
                    cr.execute(query)
                    cr.close()
                    # connection.close()
                    return cr.rowcount
        except Exception as e:
            print(e)
            time.sleep(random.randint(0, 3))
            try:
                with connection:
                    with connection.cursor() as cr:
                        cr.execute(query)
                        cr.close()
                        # connection.close()
                        return cr.rowcount
            except Exception as e:
                print(e)
                pass


def write_responses(objects):
    if objects is None:
        return None


    query_string = """INSERT INTO responses (scan_profile, request_id, url, response, headers, size, time_stamp, type,
                                                status, pp_candidate, pp_candidate_strict, cb_candidate, cb_candidate2,
                                                legal_candidate, contact_candidate, ip_address) 
                       VALUES ({0[0]}, {0[1]}, '{0[2]}', '{0[3]}', 
                   '{0[4]}', '{0[5]}', '{0[6]}', '{0[7]}', '{0[8]}', '{0[9]}', '{0[10]}', '{0[11]}', '{0[12]}', '{0[13]}',
                    '{0[14]}', '{0[15]}', '{0[16]}', {0[17]}, {0[18]}, '{0[19]}');"""

    conString = "user=hbbtv_user  password=3}cP_sA3s_fgh!j!+@@ host=194.94.127.53 port=5432 dbname=hbbtv_research"

    connection = psycopg2.connect(conString)
    cr = connection.cursor()

    # rows = []
    for obj in objects:
        # rows.append(obj)
        print(obj.keys())
        # data = (obj['scan_profile'], obj['request_id'], obj['url'], obj['method'], obj['channelname'], obj['channelid'], obj['httpVersion'],
        #        obj['status'], json.dumps(obj['headers']).replace("'", "\""), obj['postData'], obj['referrer'], obj['resource_type'], obj['etld'], obj['time_stamp'], obj['ip_address'], json.dumps(obj['cookies']).replace("'", "\""),
        #        json.dumps(obj['queryString']).replace("'", "\""), obj['headersSize'], obj['bodySize'], obj['is_known_tracker'])
        # formatted_query_string = query_string.format(data)
        # # print(formatted_query_string)
        # cr.execute(formatted_query_string)

def write_requests(objects):
    if objects is None:
        return None

    query_string = """INSERT INTO request (scan_profile, request_id, url, method, channelname, channelid, httpVersion, 
                   status, headers, postData, referrer, resource_type, etld, time_stamp,ip_address, cookies,
                   queryString, headersSize, bodySize, is_known_tracker) 
                   VALUES ({0[0]}, {0[1]}, '{0[2]}', '{0[3]}', 
                   '{0[4]}', '{0[5]}', '{0[6]}', '{0[7]}', '{0[8]}', '{0[9]}', '{0[10]}', '{0[11]}', '{0[12]}', '{0[13]}',
                    '{0[14]}', '{0[15]}', '{0[16]}', {0[17]}, {0[18]}, '{0[19]}');"""

    conString = "user=hbbtv_user  password=3}cP_sA3s_fgh!j!+@@ host=194.94.127.53 port=5432 dbname=hbbtv_research"

    connection = psycopg2.connect(conString)
    cr = connection.cursor()

    # rows = []
    for obj in objects:
        # rows.append(obj)
        data = (obj['scan_profile'], obj['request_id'], obj['url'], obj['method'], obj['channelname'], obj['channelid'], obj['httpVersion'],
               obj['status'], json.dumps(obj['headers']).replace("'", "\""), obj['postData'], obj['referrer'], obj['resource_type'], obj['etld'], obj['time_stamp'], obj['ip_address'], json.dumps(obj['cookies']).replace("'", "\""),
               json.dumps(obj['queryString']).replace("'", "\""), obj['headersSize'], obj['bodySize'], obj['is_known_tracker'])
        formatted_query_string = query_string.format(data)
        # print(formatted_query_string)
        cr.execute(formatted_query_string)

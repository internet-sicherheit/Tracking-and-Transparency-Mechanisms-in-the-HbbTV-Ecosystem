from google.cloud import bigquery
import os

datasetName = 'test'
maxRows = 500


def execQueries(queries):
    # OLD DML BIGQUERY
    from google.cloud import bigquery
    import os
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getcwd() + \
                                                   '/resources/google_bkp.json'

    print('Import job started, total rows:' + str(len(queries)))
    client = bigquery.Client()
    for q in queries:
        results = client.query(q)
        for err in results:
            print(err)
    print('Import job finished')


def exec_BQ_rows(p_tableID, p_rows, p_timeout=45):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getcwd() + \
                                                   '/resources/google_bkp.json'
    client = bigquery.Client()
    table_id = p_tableID

    print(f"Push in table {p_tableID} - {len(p_rows)}")

    try:
        errors = client.insert_rows_json(table_id, p_rows, timeout=p_timeout)
        if errors == []:
            # print(p_rows)
            print('pushed rows to BigQuery:' +
                  p_tableID + ': ' + str(len(p_rows)))
        else:
            raise Exception(
                p_tableID + ": Encountered errors while inserting rows: {}".format(errors))
            exit()
    except:
        print('error while pushing.. retry..')
        errors = client.insert_rows_json(table_id, p_rows, timeout=15)
        if errors == []:
            print('pushed rows to BigQuery:' +
                  p_tableID + ': ' + str(len(p_rows)))
        else:
            raise Exception(
                p_tableID + ": Encountered errors while inserting rows: {}".format(errors))


def stream_to_BQ(tableID, objects):
    if objects is None:
        return None
    rows = []
    for obj in objects:
        rows.append(obj)
        if len(rows) >= maxRows:
            exec_BQ_rows(tableID, rows)
            rows = []
    if len(rows) > 0:
        exec_BQ_rows(tableID, rows)


def push_error(site_id, error_type, backup_json=None):
    error = {
        'site_id': site_id,
        'error_type': error_type,
        'backup_json': backup_json
    }
    print('push_error failed: ', error)


def chunkList(lst, n):
    if lst is None:
        return None
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

import requests
from requests.auth import HTTPBasicAuth
import psycopg2
import httplib2
import os
import time
import datetime
import json
import yaml

from googleapiclient.http import MediaFileUpload
from googleapiclient import discovery
from google.oauth2 import service_account
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

from sesemail import sendEmail

value_range_body = {'values':[]}
end_date = datetime.datetime.combine(datetime.date.today(),datetime.time(tzinfo=datetime.timezone(datetime.timedelta(0))))
start_date = end_date - datetime.timedelta(days=28)

#     config = yaml.load(stream, Loader=yaml.FullLoader)

def get_credentials():
    service_account_info = json.loads(os.environ['google_api_credentials'])
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    return credentials

def unix_time_millis(dt):
    return int(datetime.datetime.timestamp(dt)*1000)

def get_api_metrics(domain, dataset_id):
    url = "https://"+domain+"/api/views/" + dataset_id + "/metrics.json?" + "start=" + str(unix_time_millis(start_date)) + "&end=" + str(unix_time_millis(end_date)) + "&method=series&slice=MONTHLY"
    r = requests.get(url,auth=HTTPBasicAuth(os.environ["socrata_username"], os.environ["socrata_password"]))
    r = r.json()
    rows_accessed_api = 0
    rows_loaded_api = 0
    try:
        for row in r:
            rows_accessed_api += int(row.get('metrics',{}).get('rows-accessed-api',0))
            rows_loaded_api += int(row.get('metrics',{}).get('rows-loaded-api',0))
    except:
        print(dataset_id)
        
    return rows_accessed_api, rows_loaded_api

def get_jpo_datasets(domain):
    url = "https://api.us.socrata.com/api/catalog/v1?provenance=official&domains="+domain+"&tags=its+joint+program+office+(jpo)&search_context="+domain
    r = requests.get(url, auth=HTTPBasicAuth(os.environ["socrata_username"], os.environ["socrata_password"]))
    r = r.json()
    return r

def lambda_handler(event, context):

    try:
        r1 = get_jpo_datasets('data.transportation.gov')
        r2 = get_jpo_datasets('datahub.transportation.gov')
        results = r1['results'] + r2['results']

        # value_range_body = {'values':[]}
        #Add parameters to connect to specific Postgres database
        # conn = psycopg2.connect(config["pg_connection_string"])
        conn = psycopg2.connect(os.environ['pg_connection_string'])
        cur = conn.cursor()
        cur.execute("SET TIME ZONE 'UTC'")
        for dataset in results:
            dataset_name = dataset['resource']['name']
            views_by_month = dataset['resource']['page_views']['page_views_last_month']
            overall_views = dataset['resource']['page_views']['page_views_total']
            downloads = dataset['resource']['download_count']
            if downloads is None:
                downloads = 0
            try:
                cur.execute("SELECT downloads FROM ipdh_metrics.dtg_metrics WHERE datetime = %s and dataset_name = %s",(start_date,dataset_name))
                previous_total = cur.fetchone()[0]
            except:
                previous_total = 0
            monthly_downloads = downloads - previous_total
            if monthly_downloads < 0:
                monthly_downloads = 0
            api_access, api_downloads = get_api_metrics(dataset['metadata']['domain'], dataset["resource"]["id"])
            cur.execute("INSERT INTO ipdh_metrics.dtg_metrics VALUES (%s,%s,%s,%s,%s,%s,%s)",(dataset_name,views_by_month,monthly_downloads,api_access,api_downloads,downloads,end_date))
            value_range_body['values'].append([dataset_name,views_by_month,monthly_downloads,api_access,api_downloads,overall_views,downloads])
        cur.execute("SELECT count(dataset_name) FROM ipdh_metrics.dtg_metrics WHERE datetime = %s",[end_date])
        numdatasets = cur.fetchone()[0]
        cur.execute("SELECT count(dataset_name) FROM ipdh_metrics.dtg_metrics WHERE datetime = %s",[end_date - datetime.timedelta(days=1)])
        prevnumdatasets = cur.fetchone()[0]
        if numdatasets != prevnumdatasets:
            print("ALERT!! Number of DTG datasets has changed. Previously there were {0} datasets, there are now {1}.\n\n".format(prevnumdatasets,numdatasets))
        conn.commit()
        cur.close()
        conn.close()

        credentials = get_credentials()
        
        service = discovery.build('sheets', 'v4', credentials=credentials)
        #Enter spreadsheet id from Google Sheets object
        spreadsheet_id = os.environ["spreadsheet_id_dtg"]
        spreadsheet_range = "A2:G" + str(len(value_range_body['values']) + 1)
        value_input_option = 'USER_ENTERED'
        request = service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=spreadsheet_range, valueInputOption=value_input_option, body=value_range_body)
        response = request.execute()
        print(response)

    except Exception as e:
        sendEmail("DTG - Lambda", str(e) )

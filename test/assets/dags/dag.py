from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime
import itertools
import os

from csv import DictReader
import geopandas as gpd
import json
import numpy as np
import pandas as pd
import itertools

from camping.mocks.request import RequestsMock
from camping.util.scraper import Scraper
from camping.util.distance import distance_merge


def _get_ridb_data(url, headers, values={}):
    response = RequestsMock.get(url, values, headers)
    if response.status_code == 200:
        result = json.loads(response.text)
        return result['RECDATA']
    return {}

@task
def get_ridb_data(url, headers, values={}):
    return _get_ridb_data(url, headers, values)

@task
def get_campsite_data(RIDB_FACILITIES_URL, facilities):
    data = []
    for facility in facilities:
        url = f"{RIDB_FACILITIES_URL}/{facility['FacilityID']}/campsites"
        campsite_data = _get_ridb_data(url, HEADERS)
        if campsite_data != {}:
            data.append(transform_campsites(campsite_data))
    return data

RIDB_FACILITIES_URL = "https://ridb.recreation.gov/api/v1/facilities"
HEADERS = {"accept": "application/json", "apikey": "key"}


file_dir = os.path.dirname(__file__)
# params is a keyword in airflow - replaced with settings
pipeline_config = [
    {'label': 'OR', 'nf_sites': f'{file_dir}/data/NF_sites/OR_sitelist.csv', 'settings':{'state': 'OR', 'activity_id': '9,6'}},
    {'label': 'WA', 'nf_sites': f'{file_dir}/data/NF_sites/WA_sitelist.csv', 'settings':{'state': 'WA', 'activity_id': '9'}}]

# Keeping transformation code seperate makes it easier to test and modify without impacting
# extraction and loading code
def transform_campsites(campsite_json):
    for i in range(len(campsite_json)):
        campsite_json[i]['AttributeDict'] = [{item['AttributeName']: item['AttributeValue']} for item in campsite_json[i]['ATTRIBUTES']]
    return campsite_json

@task 
def get_nf_data(file_name):
    nf_data = []
    with open(file_name) as f:
        reader = DictReader(f)
        for row in reader:
            sc = Scraper(row['site_url'], row['site_name'])
            nf_data.append(sc.scrape())
    return nf_data 

@task
def merge_data(ridb_data, nf_data):
    nf_df = pd.DataFrame(nf_data)
    ridb_df = pd.DataFrame(ridb_data)
    merged_sites = distance_merge(nf_df, ridb_df, 2000, 'ridb', 'nf')
    merged_sites.drop(columns=['FacilityLatitude_nf', 'FacilityLongitude_nf', 'index_nf', 'FacilityLongitude_ridb', 'FacilityLatitude_ridb', 'FacilityName', 'geometry'], inplace=True)
    combined = ridb_df.merge(merged_sites, how='left', on=['FacilityID','CampsiteID'])
    combined = combined.replace(np.nan, '')
    combined.to_json(f'data_pipeline_demo_{datetime.utcnow().isoformat()}.json', orient='records')

@task
def ridb_merge(facilities, campsites):
    df_campsites = pd.DataFrame(list(itertools.chain(*campsites)))
    df_facilities = pd.DataFrame(facilities)
    merged = df_campsites.merge(df_facilities, on='FacilityID', how='left')
    return merged.to_dict('records')
 
# Default settings applied to all tasks
default_args = {
   'owner': 'airflow',
   'depends_on_past': False,
   'email_on_failure': False,
   'email_on_retry': False,
   'retries': 0,
   'catchup': False,
   'start_date': datetime(2021, 1, 1)
}   
 
with DAG(
   dag_id='data_pipeline_demo',
   description='Data pipeline demo',
   schedule_interval=None,
   default_args=default_args
   ) as dag:
 
    # Note: It is not recommended to pass data between tasks like this, instead 
    # using distributed file systems or cloud storage
    # https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#communication
    # Instead of tasks returning data, they could return the path where the downstream
    # tasks can retrieve the data
    for item in pipeline_config:
        facilities = get_ridb_data(RIDB_FACILITIES_URL, HEADERS, item['settings'])
        campsites = get_campsite_data(RIDB_FACILITIES_URL, facilities)
        ridb_data = ridb_merge(facilities, campsites)
        nf_data = get_nf_data(item['nf_sites'])
        merge_data(ridb_data, nf_data)



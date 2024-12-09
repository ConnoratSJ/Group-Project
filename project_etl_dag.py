#!pip install airflow
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
#pip install requests
#pip install snowflake-connector-python
import requests
import snowflake.connector
import pandas as pd
import json

#The snowflake setup is like this:
#CREATE DATABASE PROJECT_DB;
#CREATE SCHEMA RAW_DATA_SCHEMA;
#CREATE SCHEMA ADHOC;
#CREATE SCHEMA ANALYTICS;
#CREATE WAREHOUSE PROJECT_WH
#    WITH
#    WAREHOUSE_SIZE = 'XSMALL'

#the Airflow has a snowflake-connection

def return_snowflake_conn():
  hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
  conn = hook.get_conn()
  return conn.cursor()



@task
def extract(start, end, zoom, density):# -> pd.DataFrame:
  data = { 'datetime':[], 'lat':[], 'long':[], 'current_speed': [], 'free_speed': []}#, 'speed_ratio': [], 'normalized_speed': []}#these are the parameters I pull from the api
  #min_speed = 200#this is used to find the minimum speed for normalization purposes
  date = datetime.now()
  api_keys = ['HU6cPuHIPouf1SEjtey3rnu1Xqd5AF6g', 'VvnPFwG3QnKuuZIG0HbAwAbfXj83ERIu', 'OuhItgHXxxnuK9A3hcOmR3pKNK2zp1GG', '033lGDhgO2lgGnVuRAtwa4Z7GeX99USv', 'driiBrint2vBw6LB1dILDuSyGFnuUbgl']
  #api_keys = ['HU6cPuHIPouf1SEjtey3rnu1Xqd5AF6g']
  for i in range(density):
    
    for j in range(density):
      
      api_key = api_keys.pop(0)
      
      api_keys.append(api_key)
      
      url =f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/{zoom}/json?key={api_key}&point={start[0] - (i * (start[0] - end[0])/(density))},{start[1] - (j * (start[1] - end[1])/(density))}"
      r = requests.get(url)
      api_call = r.json()
      
      if('flowSegmentData' in api_call.keys()):#checks if something returned, sometimes it is just null
        coords = api_call['flowSegmentData']
        '''if(coords['currentSpeed'] < min_speed):#finding minimum speed
          min_speed = coords['currentSpeed']'''
        for coord in coords['coordinates']['coordinate']:
          data['datetime'].append(date)#adding datetime
          data['lat'].append(coord['latitude'])
          data['long'].append(coord['longitude'])
          data['current_speed'].append(coords['currentSpeed'])
          data['free_speed'].append(coords['freeFlowSpeed'])
          #data['speed_ratio'].append(coords['currentSpeed']/coords['freeFlowSpeed'])
          #data['normalized_speed'].append(coords['currentSpeed'])

  '''for i in range(len(data['normalized_speed'])):
    data['normalized_speed'][i] = data['normalized_speed'][i]/(min_speed)'''

  df = pd.DataFrame(data)

  df = df.drop_duplicates(subset=['lat', 'long'])#there is no primary key uniqueness, but lat, long and timestamp serve to uniequely identify each point on the map at the given timestamp, so to ensure idempotency, we prune duplicate lat long entries here, which occurs for each timestamp, and we then end up with a unique date, lat, long combination for each datapoint in our data warehouse

  return df




@task
def load_data(cursor, data : pd.DataFrame, table):
  sql_query = f'CREATE TABLE IF NOT EXISTS {table}(date TIMESTAMP_NTZ, lat FLOAT, long FLOAT, current_speed FLOAT, free_speed FLOAT);'#, speed_ratio FLOAT, normalized_speed FLOAT);'
  #sql_query = f'CREATE TABLE IF NOT EXISTS {table}(date TIMESTAMP_NTZ, lat FLOAT, long FLOAT, current_speed FLOAT, free_speed FLOAT, speed_ratio FLOAT);'
  cursor.execute(sql_query)

  for index, row in data.iterrows():
    #insert_sql = f"INSERT INTO {table}(date, lat, long, current_speed, free_speed, speed_ratio, normalized_speed) VALUES(TO_TIMESTAMP_NTZ('{row['datetime']}'), {row['lat']}, {row['long']}, {row['current_speed']}, {row['free_speed']}, {row['speed_ratio']}, {row['normalized_speed']});"
    insert_sql = f"INSERT INTO {table}(date, lat, long, current_speed, free_speed) VALUES(TO_TIMESTAMP_NTZ('{row['datetime']}'), {row['lat']}, {row['long']}, {row['current_speed']}, {row['free_speed']});"
    cursor.execute(insert_sql)


with DAG(
    dag_id = 'project_etl_dag',
    start_date = datetime(2024,9,21),
    catchup=False,
    tags=['ETL'], 
    schedule = '00,15,30,45 7-18 * * *'#this should run every 15 minutes between 7 am and 7 pm, I have 5 api keys that work I had to adjust the hours to UTC
) as dag:
    
    target_table = "PROJECT_DB.RAW_DATA_SCHEMA.TRAFFIC_DATA"


    
    start = Variable.get("start")
    start = start.split(", ")
    start = tuple([float(i) for i in start])

    end = Variable.get("end")
    end = end.split(", ")
    end = tuple([float(i) for i in end])

    zoom = Variable.get("zoom")
    zoom = int(zoom)

    density = Variable.get("density")
    density = int(density)

    cursor = return_snowflake_conn()
    
    df = extract(start, end, zoom, density)
    load_data(cursor, df, target_table)
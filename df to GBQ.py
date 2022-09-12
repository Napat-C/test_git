from google.cloud import bigquery
import os
import pandas_gbq
import pandas as pd
import requests
import gcsfs

def Thai_AQI():
    
    url = "http://air4thai.pcd.go.th/services/getNewAQI_JSON.php"
    r = requests.get(url)
    
    if  r.status_code == 200:
        
        df = pd.read_json(url, orient='records')
        dp = pd.json_normalize(df['stations'])
        cols = dp.columns
        cols=cols.str.replace('LastUpdate.', '')
        cols=cols.str.replace('.value', '')
        cols=cols.str.replace('.aqi', '')
        cols=cols.str.replace('AQI.Level', 'AQI_Level')
        dp.columns=cols
        dp.reset_index(inplace=False)
        d_out = dp[['stationID','date','time','PM25','PM10','CO','NO2','SO2','AQI', 'AQI_Level','lat','long']]
             
    else: 
        print("API Error")
       
    return d_out

def write_to_gbq(df,Table_ID,Project_ID,table_schema):
     ## Get BiqQuery Set up
    client = bigquery.Client()
    table = client.get_table(Table_ID)
    pandas_gbq.to_gbq(df, destination_table = Table_ID, project_id = Project_ID, 
                          if_exists='append', table_schema=table_schema)
    
Table_ID = 'commondata-341406.air4thaiaqidata.aqi_data_thailand'
Project_ID = 'commondata-341406' 
table_schema = [{'name':'stationID', 'type':'STRING'},
                {'name':'date', 'type':'DATE'},
                {'name':'time', 'type':'STRING'},
                {'name':'PM25', 'type':'STRING'},
                {'name':'PM10', 'type':'STRING'}, 
                {'name':'CO', 'type':'STRING'},
                {'name':'NO2', 'type':'STRING'},
                {'name':'SO2', 'type':'STRING'},
                {'name':'AQI', 'type':'INTEGER'},
                {'name':'AQI_Level','type':'INTEGER'},
                {'name':'Lat','type':'STRING'},
                {'name':'Long','type':'STRING'}]
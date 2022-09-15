import pandas as pd
from datetime import datetime
from google.oauth2.service_account import Credentials

def str_to_date(str):
    return datetime.strptime(str, '%Y-%m-%d').date()


data = [{'DATE': str_to_date('2020-01-01'), 'TYPE': 'TypeA', 'SALES': 1000},
        {'DATE': str_to_date('2020-01-01'), 'TYPE': 'TypeB', 'SALES': 200},
        {'DATE': str_to_date('2020-01-01'), 'TYPE': 'TypeC', 'SALES': 300},
        {'DATE': str_to_date('2020-02-01'), 'TYPE': 'TypeA', 'SALES': 700},
        {'DATE': str_to_date('2020-02-01'), 'TYPE': 'TypeB', 'SALES': 400},
        {'DATE': str_to_date('2020-02-01'), 'TYPE': 'TypeC', 'SALES': 500},
        {'DATE': str_to_date('2020-03-01'), 'TYPE': 'TypeA', 'SALES': 300},
        {'DATE': str_to_date('2020-03-01'), 'TYPE': 'TypeB', 'SALES': 900},
        {'DATE': str_to_date('2020-03-01'), 'TYPE': 'TypeC', 'SALES': 100}]
df = pd.DataFrame(data)

# Define target table in BQ
target_table = "YOUR_DATA_SET.pandas"
project_id = "sonorous-seat-362304"
credential_file = r"C:\Users\NapatCota\Downloads\sonorous-seat-362304-8df46a7a8c0b.json"
credential = Credentials.from_service_account_file(credential_file)
# Location for BQ job, it needs to match with destination table location
job_location = "asia-southeast1"

# Save Pandas dataframe to BQ
df.to_gbq(target_table, project_id=project_id, if_exists='replace',
          location=job_location, progress_bar=True, credentials=credential)
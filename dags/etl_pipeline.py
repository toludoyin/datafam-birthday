import os
import pathlib
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
import gspread
import pandas as pd
import psycopg2

import json
import pendulum
from airflow.decorators import dag, task

# environment variables
load_dotenv()
service_account = os.environ.get("SERVICE_ACCOUNT_KEY_PATH")
sheet_url = os.getenv("sheet_link")
user = os.environ.get("POSTGRES_USER")
password = os.environ.get("POSTGRES_PASSWORD")
dbname = os.environ.get("POSTGRES_DB")

# google sheet credentials
scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
credentials = Credentials.from_service_account_file(service_account, scopes=scopes)
client = gspread.authorize(credentials)

@dag(
    schedule="00 01 * * *",
    start_date=pendulum.datetime(2024, 8, 1, tz="UTC"),
    catchup=False,
    tags=["datafam"],
)

def datafam_birthday():
   @task
   def get_data_from_sheet(sheet_url, sheet_index=0):
      sheet = client.open_by_url(sheet_url).get_worksheet(sheet_index)
      data = sheet.get_all_values()
      return data

   @task
   def transform_data(get_sheet_data):
      if get_sheet_data:
         datafam_df = pd.DataFrame(get_sheet_data[1:], columns=get_sheet_data[0])

      # generate date of birth from date 
      datafam_df['year'] = '2024'
      datafam_df['month_num'] = pd.to_datetime(datafam_df['Which month were you born?'], format='%B').dt.month
      datafam_df["Date of Birth"] = datafam_df["Which day of the month were you born?"].astype(str) + "-" + datafam_df["month_num"].astype(str) + "-" + datafam_df["year"]
      datafam_df["Date of Birth"] = pd.to_datetime(datafam_df["Date of Birth"], format='%d-%m-%Y', errors='coerce')

      # remove trailing @symbol
      datafam_df['Twitter Handle'] = datafam_df['Twitter Handle'].str.replace(r'\@', '', regex=True)
      return datafam_df

   # @task
   # def load_data_to_db(datafam_df):
   #    try:
   #       table_name = 'datafam_birthday'
   #       connection = psycopg2.connect(dbname=dbname, user=user, password=password, host= 'postgres', port= '5434')
   #       cursor = connection.cursor()

   #       create_table = f"CREATE TABLE IF NOT EXISTS {table_name} \
   #                          ({','.join([f'{column_name} VARCHAR' for column_name in datafam_df.columns])});"
   #       cursor.execute(create_table)
   #       connection.commit()
         
   #       # insert column and values to db
   #       column = ', '.join(datafam_df.columns)
   #       placeholders = ', '.join(['%s'] * len(datafam_df.columns)) # number of columns
   #       query = f"INSERT INTO {table_name} ({column}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

   #       for row in datafam_df.values.tolist():
   #             cursor.execute(query, row)
   #       connection.commit()

   #    except (Exception, psycopg2.Error) as error:
   #       print('Error while connecting to PostgreSQL', error)

   #    finally:
   #       print("Data inserted successfully")

   get_sheet_data = get_data_from_sheet(sheet_url)
   transform_data = transform_data(get_sheet_data)
#    # load = load_data_to_db(load)

datafam_birthday()
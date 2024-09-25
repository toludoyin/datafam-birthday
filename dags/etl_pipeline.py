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
from airflow.providers.postgres.hooks.postgres import PostgresHook

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
load_dotenv()

class GoogleSheetClient:
   def __init__(self, service_account_key_path, scopes):
      self.credentials = Credentials.from_service_account_file(service_account_key_path, scopes=scopes)
      self.client = gspread.authorize(self.credentials)

   def get_data_from_sheet(self, sheet_url, sheet_index=0):
      sheet = self.client.open_by_url(sheet_url).get_worksheet(sheet_index)
      data = sheet.get_all_values()
      return data
   
class DataTransformer:
   def __init__(self, data):
      self.df = pd.DataFrame(data[1:], columns=data[0])

   def transform_data(self):
      # create date of birth
      self.df['Year'] = '2024'
      self.df['Month Num'] = pd.to_datetime(self.df['Which month were you born?'], format='%B').dt.month
      self.df['Date of Birth'] = self.df['Which day of the month were you born?'].astype(str) + '-' + self.df['Month Num'].astype(str) + '-' + self.df['Year']
      self.df['Date of Birth'] = pd.to_datetime(self.df['Date of Birth'], format='%d-%m-%Y', errors='coerce')
      # self.df['Timestamp'] = pd.to_datetime(self.df['Timestamp'])

      # drop duplicate and null rows
      self.df = self.df[~((self.df['Twitter Handle'] == '') & (self.df['LinkedIn Profile'] == ''))]
      self.df['Twitter Handle'] = self.df['Twitter Handle'].str.lower().str.strip()
      
      # modify twitter handle and replace links with username
      self.df['Twitter Handle'] = self.df['Twitter Handle'].str.replace('＠', '@', regex=False)
      self.df['Twitter Handle'] = self.df['Twitter Handle'].str.extract(r'(?:https?://(?:x\.com|twitter\.com)/)?@?([^/]+)')
      self.df['Twitter Handle'] = self.df['Twitter Handle'].fillna('')
      self.df = self.df.drop_duplicates(subset=['Twitter Handle', 'LinkedIn Profile'], keep='last')
      empty_handles = self.df[self.df['Twitter Handle'] == '']
      non_empty_handles = self.df[self.df['Twitter Handle'] != '']
      non_empty_handles = non_empty_handles.drop_duplicates(subset='Twitter Handle', keep='last')
      self.df = pd.concat([non_empty_handles, empty_handles]).reset_index(drop=True)
      # redefine column name
      self.df.columns = [col.replace(' ', '_').replace('?', '').lower() for col in self.df.columns]
   
      return self.df

class PostgresLoader:
   def __init__(self, postgres_conn_id):
      self.postgres_conn_id=postgres_conn_id

   def load_data_to_db(self, data_df):
      stg_table_name = 'stg_datafam_birthday'
      table_name = 'datafam_birthday'
      dtype_mapping = {
         'int32' : 'integer',
         'string' : 'varchar',
         'float' : 'float',
         'boolean' : 'boolean',
         'datetime64[ns]' : 'date'
      }
      column_mapping = ','.join([f'{col_name} {dtype_mapping.get(str(dtype), "VARCHAR")}' for col_name, dtype in data_df.dtypes.items()])
      columns = ', '.join(data_df.columns)
      placeholders = ', '.join(['%s'] * len(data_df.columns))
      
      try:
         hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
         connection = hook.get_conn()
         cursor = connection.cursor()
         
         # staging table
         cursor.execute(f"DROP TABLE IF EXISTS {stg_table_name};")
         create_stg_table = f"""CREATE TEMPORARY TABLE {stg_table_name}({column_mapping});"""
         cursor.execute(create_stg_table)
         insert_query = f"""INSERT INTO {stg_table_name} ({columns}) VALUES ({placeholders})"""
         for row in data_df.values.tolist():
            cursor.execute(insert_query, row)

         # main table
         create_main_table = f"""CREATE TABLE IF NOT EXISTS {table_name} ({column_mapping});"""
         cursor.execute(create_main_table)
         merge_query = f"""
            MERGE INTO {table_name} AS main
            USING {stg_table_name} AS stg
            ON main.twitter_handle = stg.twitter_handle
            AND main.linkedin_profile = stg.linkedin_profile
            WHEN NOT MATCHED THEN
               INSERT ({columns}) VALUES ({', '.join([f'stg.{col}' for col in data_df.columns])})
            WHEN MATCHED THEN
               UPDATE SET {', '.join([f'{col} = stg.{col}' for col in data_df.columns])};
         """
         cursor.execute(merge_query)
         connection.commit()
         print("Data inserted successfully")

      except (Exception, psycopg2.Error) as error:
         print('Error while connecting to PostgreSQL', error)
         connection.rollback()

@dag(
   schedule="00 01 * * *",
    start_date=pendulum.datetime(2024, 8, 1, tz="UTC"),
    catchup=False,
    tags=["datafam_dag"],
)

def datafam_birthday():
   @task
   def etl_flow():
      # initialise classes
      service_account = os.environ.get("SERVICE_ACCOUNT_KEY_PATH")
      sheet_url = os.getenv("sheet_link")
      scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
      postgres_conn_id='datafam_birthday_conn'

      # google sheet operations
      sheet_client = GoogleSheetClient(service_account, scopes)
      sheet_data = sheet_client.get_data_from_sheet(sheet_url)

      # data transformation
      transformer = DataTransformer(sheet_data)
      transformed_data = transformer.transform_data()

      # load data to postgres db
      loader = PostgresLoader(postgres_conn_id)
      loader.load_data_to_db(transformed_data)
   etl_flow()

datafam_birthday()

# import libraries
import os
import pathlib
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd

load_dotenv()
path = pathlib.Path(__file__).parent
data_path = path.joinpath('../').resolve()
sheet_name = os.getenv('sheet_url')

scopes = [
   'https://www.googleapis.com/auth/spreadsheets', 
   'https://www.googleapis.com/auth/drive'
]

credentials = Credentials.from_service_account_file(
   data_path.joinpath('datafam-cred.json'),
   scopes=scopes)
client = gspread.authorize(credentials)

# pull data from spreadsheet
def get_sheet_data(sheet_name, sheet_index=0):
   sheet = client.open_by_url(sheet_name).get_worksheet(sheet_index)
   data_fam = sheet.get_all_values()
   
   # convert to dataframe
   dataframe = pd.DataFrame(data_fam)
   dataframe.head()

def load_data():




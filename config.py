import os,gspread
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

local_ip         = os.getenv('LOCAL_IP','192.168.10.9')
server_port      = int(os.getenv('SERVER_PORT',3000))
file_to_cloud    = os.getenv('FILE_TO_CLOUD','12qKrKeMAj9uuo97sp6wQ3eutjBMug9zHSBkIeUaxryk')
arc_folder_id    = os.getenv('ARC_FOLDER_ID','1S8CF33dX4p-opgiVIizuZB-BdvyXhR3O')
delay            = int(os.getenv("DELAY", 1))

def get_drive_service():
    creds = Credentials.from_service_account_file("creds/credentials.json",
                                                  scopes=["https://www.googleapis.com/auth/drive"])
    drive_service = build('drive', 'v3', credentials=creds)
    return drive_service

def get_sheet_service():
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    creds = Credentials.from_service_account_file("creds/credentials.json", scopes=scopes)
    client = gspread.authorize(creds)
    return client
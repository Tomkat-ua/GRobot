######## FOR Excel
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
import fbextract,datetime,io,os
from google.oauth2.service_account import Credentials
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload
import pandas as pd

import to_cloud

tmp_dir = 'tmp/'

# 🔐 Авторизація до Google Drive
SCOPES = ['https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_file('creds/credentials.json', scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=creds)


#✅ 1. Знайти файл на Google Drive
def finde_on_drive():
    # 🔍 Знайти всі .xlsx файли в певній папці
    FOLDER_ID = '13ixPu84zGwKSqMOZUUvcXjryrCljUbpV'
    query = f"'{FOLDER_ID}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])
    files_list = {}
    for file in files:
        file_id = file['id']
        file_name = file['name']
        if file_name.startswith("UPD"):
            files_list[file_id] = file_name
    return files_list


# ✅ 2. Завантажити файл .xlsx локально
def load_file(file_id,file_name):
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    with open(tmp_dir+file_name, "wb") as f:
        f.write(fh.getbuffer())


def write_changes(file_id,file_name):
    def write_to_db(new_rows):
        # ✅ 4. Внести зміни в БД
        con = fbextract.get_connection()
        cur = con.cursor()
        for _, record in new_rows.iterrows():
            print(record['військовий номер'])
            cur.execute(' execute procedure LOAD_DATA (?,?,?,?,?,?,?,?)',
                        [file_name, record['військовий номер'], record['підрозділ'], record['статус'], record['тех стан'],
                         record['водій'], record['локація'], record['примітки']])
            # result = cur.fetchone()
            # print(f"Оновлено {result[0]} записів")
        con.commit()
        con.close()

    # ✅ 3. Прочитати змінені рядки
    df = pd.read_excel(tmp_dir +file_name)
    new_rows = df[df['sync_status'] == 'Змінено']
    print(len(new_rows))
    if len(new_rows) > 0:
        write_to_db(new_rows)
        to_cloud.to_cloud()
    # ✅ 5. Видалити локальний файл
    os.remove(tmp_dir+file_name)
    # ✅ 6.Перейменувати файл на Google Drive
    new_name = f"processed_{file_name}"
    drive_service.files().update(fileId=file_id, body={"name": new_name}).execute()

def main_cycle():
    try:
        files_list = finde_on_drive()
        for file_id in files_list:
            file_name = files_list[file_id]
            print(file_id, file_name)
            load_file(file_id,file_name)
            write_changes(file_id,file_name)
    except Exception as e:
        print(str(e))



# main_cycle() # <--  test mode
######## FOR Excel
import fbextract,datetime,io,os
from googleapiclient.http import MediaIoBaseDownload
import pandas as pd
import to_cloud,from_booking,config,move_file

tmp_dir = 'tmp/'

drive_service = config.get_drive_service()

#✅ 1. Знайти файл на Google Drive
def finde_on_drive():
    # 🔍 Знайти всі .xlsx файли в певній папці
    folder_id = '13ixPu84zGwKSqMOZUUvcXjryrCljUbpV'
    query = f"'{folder_id}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])
    files_list = {}
    for file in files:
        file_id = file['id']
        file_name = file['name']
        print(f"⏩{datetime.datetime.now()} - {file_name}")
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

def write_changes(file_id,file_name,mode =0):
    def write_to_db(new_rows):
        # ✅ 4. Внести зміни в БД
        con = fbextract.get_connection()
        cur = con.cursor()
        for _, record in new_rows.iterrows():
            print(f"⏩{datetime.datetime.now()} - оброблено {record['військовий номер']}")
            cur.execute(' execute procedure LOAD_DATA (?,?,?,?,?,?,?,?)',
                        [file_name, record['військовий номер'], record['підрозділ'], record['статус'], record['тех стан'],
                         record['водій'], record['локація'], record['примітки']])
        con.commit()
        con.close()

    # ✅ 3. Прочитати змінені рядки
    df = pd.read_excel(tmp_dir +file_name)
    new_rows = df[df['sync_status'] == 'Змінено']
    print(f"⏩{datetime.datetime.now()} - змін всього {len(new_rows)}")
    if len(new_rows) > 0:
        write_to_db(new_rows)
        from_booking.from_booking(file_name)
        to_cloud.to_cloud(config.file_to_cloud)
    if mode == 0:
        # ✅ 5. Видалити локальний файл
        os.remove(tmp_dir+file_name)
        # ✅ 6.Перейменувати файл на Google Drive
        new_name = f"processed_{datetime.datetime.now()}_{file_name}"
        drive_service.files().update(fileId=file_id, body={"name": new_name}).execute()
        print(f"✅{datetime.datetime.now()} - оброблено {file_name} ")
        print(f"✅{datetime.datetime.now()} - перейменовано  {new_name} ")
        move_file.move_file(file_id)
        print(f"✅{datetime.datetime.now()} - переміщено до архіву")

def main_cycle():
    print(f"⏩{datetime.datetime.now()} ======================================")
    try:
        files_list = finde_on_drive()
        for file_id in files_list:
            file_name = files_list[file_id]
            load_file(file_id,file_name)
            write_changes(file_id,file_name)
        print(f"⏩{datetime.datetime.now()} - {len(files_list)} file(s) for process ")
    except Exception as e:
        print(f"❗{datetime.datetime.now()} - ERROR main_cycle {str(e)}")



######## test area
# main_cycle() # <--  test mode

# write_changes('1560j0LBhnGj_76koAEJSPFW8VpeWgKnI','UPD_Автомобілі_1ЄБ.xlsx',1)
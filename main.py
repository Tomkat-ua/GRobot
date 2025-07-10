
import pandas as pd,io,os,time,fbextract,datetime,warnings
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload





delay = os.getenv("DELAY", 1)

def readdisk():
    # 🔐 Авторизація до Google Drive
    SCOPES = ['https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('credentials.json', scopes=SCOPES)
    drive_service = build('drive', 'v3', credentials=creds)

    # 🔍 Знайти всі .xlsx файли в певній папці
    FOLDER_ID = '13ixPu84zGwKSqMOZUUvcXjryrCljUbpV'

    query = f"'{FOLDER_ID}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])
    # ОСНОВНИЙ ЦИКЛ - Для кожного  файлу ===============================================================================
    print(f"⏩{datetime.datetime.now()} ==== BEGIN ===============================================================")
    for file in files:

        file_id = file['id']
        file_name = file['name']

        print(f"⏩{datetime.datetime.now()} - Знайдено файл: {file_name}")
        if file_name.startswith("processed"):
            print(f"⏩{datetime.datetime.now()} - Цей файл вже оброблено - пропускаю")
        else:
            if file_name.startswith("UPD"):
                print(f"⏩{datetime.datetime.now()} -- Файл оновлення даних - починаю завантаження")
                read_update(file_name,file_id,drive_service)
            if file_name.startswith("INS"):
                print(f"⏩{datetime.datetime.now()} -- Файл додавання даних - поки що не реалізовано, пропускаю")
            else:
                print(f"⏩{datetime.datetime.now()} -- невідомий файл - пропускаю")

    print(f"⏩{datetime.datetime.now()} ===== END ================================================================")
#######################################################################################################################
def read_update(file_name,file_id,drive_service):
    print(f"⏩{datetime.datetime.now()} - Оновлення з файлу: {file_name}")

    print(f"📥️{datetime.datetime.now()} -- Завантаження файлу")
    #  Завантаження файлу
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()

    print(f"🔌{datetime.datetime.now()} -- Підключення до Firebird")
    # 🔌 Підключення до Firebird
    con = fbextract.get_connection()
    cur = con.cursor()

    print(f"🔌{datetime.datetime.now()} --- Запис файлу в UPD_FILES")
    file_bytes = fh.getvalue()  # <-- BLOB----------------------
    cur.execute('execute procedure LOAD_FILE (?,?)', [file_name, file_bytes])
    fh.seek(0)

    print(f"📊{datetime.datetime.now()} --- Читання Excel")
    # 📊 Читання Excel
    warnings.filterwarnings("ignore", category=UserWarning)
    df = pd.read_excel(fh)
    # Для кожного рядка в файлі --------------------------------------------------------------
    for _, row in df.iterrows():
        cur.execute(' execute procedure LOAD_DATA (?,?,?,?,?,?,?,?)',
        [file_name, row.iloc[0], row.iloc[1], row.iloc[2], row.iloc[3], row.iloc[4], row.iloc[5], row.iloc[6]])
    # -----------------------------------------------------------------------------------------
    print(f"📊{datetime.datetime.now()} --- Збереження записів в UPD_DATA")
    con.commit()

    # оновлюємо дані в БД
    print(f"➡️{datetime.datetime.now()} --- Оновлення в CARS ")
    res = []
    try:
        cur.execute("EXECUTE PROCEDURE CARS_UPD")
        res = cur.fetchone()
        print(f"✅{datetime.datetime.now()} --- Оновлено записів: {res[0]}")

        # якщо є зміни - оновлюємо таблицю загальну в хмарі
        if res[0] != None:
            con.commit()
            writefile()
            # 🏷 Перейменування файлу
            new_name = 'processed_' + file_name
            drive_service.files().update(fileId=file_id, body={'name': new_name}).execute()
            print(f"✅{datetime.datetime.now()} Завантажено та перейменовано: {new_name}")
    except Exception as e:
        print(f"❌{datetime.datetime.now()} ERROR: {str(e)}")
    cur.close()
    con.close()
    print(f"✅{datetime.datetime.now()} -- Закрите підключення")
####################################################################################################################



def writefile():

    # Параметри доступу
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    # Авторизація
    creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
    client = gspread.authorize(creds)
    # Відкрити таблицю за назвою ## 1iIhAaIoHz2bU18QKw6xis3MxohBDxwmkCHlb852NFco
    # sh = gc.open("transport")  # або .open_by_url(...)
    # sh = gc.open_by_key("1iIhAaIoHz2bU18QKw6xis3MxohBDxwmkCHlb852NFco")
    # ✅ ВСТАВ СЮДИ URL (обов’язково повний!)
    url ='https://docs.google.com/spreadsheets/d/10Ft5t3q6QMCh_ou9mqfufYd7l6Ic70JdhUNOdsQFpHk/edit?gid=0#gid=0'
    sh = client.open_by_url(url)

    ############################################
    print(f"➡️{datetime.datetime.now()} Запис дельти ... ")
    # Дані для запису
    sql = ' select * from v_cars'
    rows = fbextract.get_data(sql,[])
    # Очистити старі дані (опційно)
    # Відкриваємо лист
    sheet = sh.sheet1  # перший аркуш
    # sheet = client.open("Products Sync").sheet1

    # ❗ Не очищаємо заголовки — тільки дані
    # Отримаємо кількість рядків, видалимо старі дані з другого рядка вниз
    num_rows = len(sheet.get_all_values())
    if num_rows > 1:
        sheet.batch_clear([f"A2:Z{num_rows}"])

    # Записати заголовки + дані
    # for row in rows:
    #     worksheet.append_row(list(row))
    # values = ([["CAR_ID","MIL_NUM","CARTYPE_NAME","BRAND","DIVISION_NAME","STATUS_NAME","STATE_NAME","SEATS","W_FORM","DRIVER","LOCATION","NOTES"]]
    #           + [list(row) for row in rows])
    values = [list(row) for row in rows]
    sheet.update(range_name="A2", values=values)
    print(f"✅️{datetime.datetime.now()} Запис ОК ")


# schedule.every(1).minutes.do(readfiles())
# if __name__ == '__main__':

# readdisk()

while True:
    # schedule.run_pending()
    try:
        readdisk()
    #     readfiles()
        time.sleep(int(delay)*60)
    #     # writefile()
    except Exception as e:
        print(f"⛔️{datetime.datetime.now()} -{str(e)} ")

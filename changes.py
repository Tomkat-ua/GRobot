import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
import fbextract,datetime
from google.oauth2.service_account import Credentials

def proc_file(file_name,sheet):
    # 2. Відкриваємо аркуш
    # sheet = client.open("Автомобілі_1ЄБ").sheet1
    # sheet = client.open(file_name).sheet1
    data = sheet.get_all_values()

    # 3. Підключення до Firebird
    con = fbextract.get_connection()
    cur = con.cursor()

    # 4. Обробка змінених рядків
    ch = 0 ## кількість змін в файлі
    header = data[0]
    for i, row in enumerate(data[1:], start=2):  # з другого рядка
        if len(row) < len(header):
            row += [''] * (len(header) - len(row))  # заповнення порожніх
        record = dict(zip(header, row))
        if record.get('sync_status') == 'Змінено':
            # Вставка або оновлення — приклад:
            # cur.execute(" UPDATE CARS set driver = ? WHERE car_id = ?", [record['водій'],int(record['car_id'])])
            # print(row)
            cur.execute(' execute procedure LOAD_DATA (?,?,?,?,?,?,?,?)',
             [file_name, record['військовий номер'], record['підрозділ'], record['статус'], record['тех стан'],
              record['водій'], record['локація'],record['примітки']])

            # Очищаємо мітку
            # sheet.update(f'E{i}', [['ок']])  # sync_status в колонці E
            sheet.update(range_name=f'A{i}', values=[['Oк']])
            ch = ch + 1
            print(f"  ✅️{datetime.datetime.now()}: Оновлено: рядок {i}, номер  {record['військовий номер']}")
    # 5. Завершення
    print(f"  ✅️{datetime.datetime.now()}: {file_name} - Оброблено, зміни - {ch}")
    con.commit()
    con.close()
    return ch


def to_cloud():
    try:
        print(f"⏩{datetime.datetime.now()} ==== BEGIN TO CLOUD ======================================================")
        result = []
        # Параметри доступу
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        # Авторизація
        creds = Credentials.from_service_account_file("creds/credentials.json", scopes=SCOPES)
        client = gspread.authorize(creds)
        # Відкрити таблицю за назвою ## 1iIhAaIoHz2bU18QKw6xis3MxohBDxwmkCHlb852NFco
        # sh = gc.open("transport")  # або .open_by_url(...)
        # sh = gc.open_by_key("1iIhAaIoHz2bU18QKw6xis3MxohBDxwmkCHlb852NFco")
        # ✅ ВСТАВ СЮДИ URL (обов’язково повний!)
        url ='https://docs.google.com/spreadsheets/d/10Ft5t3q6QMCh_ou9mqfufYd7l6Ic70JdhUNOdsQFpHk/edit?gid=0#gid=0'
        sh = client.open_by_url(url)

        ############################################
        result.append(f"➡️{datetime.datetime.now()} -- Запис дельти ... ")
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
        # print(f"✅️{datetime.datetime.now()} Запис ОК ")
        result.append(f"✅️{datetime.datetime.now()} -- Запис ОК ")
        for row in result:
            print(row)
        print(f"⏩{datetime.datetime.now()} ==== END TO CLOUD ========================================================")
        return result
    except Exception as e:
        print(f"⏩{datetime.datetime.now()} ==== BEGIN TO CLOUD ======================================================")
        return str(e)

def from_cloud():
    print(f"⏩{datetime.datetime.now()} ==== BEGIN FROM CLOUD ======================================================")
    # 1. Авторизація Google Sheets
    scope = ['https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('creds/credentials.json', scope)
    client = gspread.authorize(creds)
    drive_service = build('drive', 'v3', credentials=creds)
    # print(drive_service)

    # 🔍 Знайти всі файли в певній папці
    files = drive_service.files().list(q="mimeType='application/vnd.google-apps.spreadsheet'",
                                        fields="files(id, name, mimeType)").execute()
    gch = 0
    for f in files.get('files', []): # <-- цикл по списку файлів
        file_name = f['name']
        if file_name.startswith("Автомобілі"):
            print(f" ⏩{datetime.datetime.now()}: {f['name']} - {f['id']}")
            sheet = client.open(file_name).sheet1
            gch = gch + proc_file(file_name,sheet) # <-- обробка одного  файлу
    print(f" ⏩{datetime.datetime.now()} - Всього змін - {gch}")
    print(f"⏩{datetime.datetime.now()} ===== END FROM CLOUD========================================================")
    return gch

def main_cycle():
    try:
        ch_cloud = from_cloud()
        if ch_cloud > 0:
            to_cloud()
    except Exception as e:
        print(str(e))


# from_cloud() # <--  test mode
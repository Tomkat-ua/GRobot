import gspread
import fbextract,datetime
from google.oauth2.service_account import Credentials



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
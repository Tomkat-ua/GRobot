import fbextract, gspread,datetime
from google.oauth2.service_account import Credentials



def to_cloud(file_to_cloud):
    result=[]
    try:
        # 📌 Параметри
        DAYS = 7
        # SPREADSHEET_ID = '1DsiSD8rZbwr-rP9K86H2n6h7ijj4IXEO9G4g66dzSb8'  # твій файл
        # SHEET_NAME = 'Список'  # або назва аркуша

        # 🔐 Авторизація
        # gc = gspread.service_account(filename='creds/credentials.json')
        # sh = gc.open_by_key(SPREADSHEET_ID)
        # sheet = sh.worksheet(SHEET_NAME)

        # Параметри доступу
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        # Авторизація
        creds = Credentials.from_service_account_file("creds/credentials.json", scopes=SCOPES)
        client = gspread.authorize(creds)
        # Відкрити таблицю за назвою
        # sh = gc.open("transport")  # або .open_by_url(...)
        # sh = gc.open_by_key("1iIhAaIoHz2bU18QKw6xis3MxohBDxwmkCHlb852NFco")
        # url ='https://docs.google.com/spreadsheets/d/1DsiSD8rZbwr-rP9K86H2n6h7ijj4IXEO9G4g66dzSb8/edit?gid=0#gid=0'
        # sh = client.open_by_url(url)
        sh = client.open_by_key(file_to_cloud)
        sheet = sh.sheet1
        result.append(f"➡️{datetime.datetime.now()} --🔐 Авторизація  ОК")

        # 📦 Отримання даних із Firebird
        con = fbextract.get_connection()
        cur = con.cursor()
        result.append(f"➡️{datetime.datetime.now()} --📦 Отримання даних із Firebird ....")

        # 🚗 Основна таблиця
        cur.execute("SELECT * FROM V_CARS order by car_id")
        main_data = cur.fetchall()
        result.append(f"➡️{datetime.datetime.now()} --- 🚗 Основна таблиця ....")

        # 🧠 Заголовки
        headers = [desc[0] for desc in cur.description]
        result.append(f"➡️{datetime.datetime.now()} --- 🧠 Заголовки ....")

        # 📅 Діапазон дат на тиждень
        start_date = datetime.datetime.now().date()
        date_headers = [(start_date + datetime.timedelta(days=i)).strftime('%d.%m.%Y') for i in range(DAYS)]
        result.append(f"➡️{datetime.datetime.now()} --- 📅 Діапазон дат на тиждень ....")

        # 📖 Бронювання
        cur.execute("SELECT mil_num, date_, bat_order FROM v_BOOKING")
        rows = cur.fetchall()
        result.append(f"➡️{datetime.datetime.now()} --- 📖 Бронювання ....")

        # 🔍 Обробка бронювання
        booking_dict = {}
        for mil_num, date_, bat_order in rows:
            date_str = date_.strftime('%d.%m.%Y')
            booking_dict.setdefault(mil_num.strip(), {})[date_str] = bat_order
        result.append(f"➡️{datetime.datetime.now()} --- 🔍 Обробка бронювання ....")

        # 🧱 Побудова повної таблиці
        output = []
        header_row = headers + ['РЕЗЕРВ'] + date_headers
        output.append(header_row)


        for row in main_data:
            mil = row[1].strip()
            bookings = booking_dict.get(mil, {})
            reserved_flag = 'ТАК' if bookings else ''
            booking_row = [bookings.get(d, '') for d in date_headers]
            output.append(list(row) + [reserved_flag] + booking_row)
        result.append(f"➡️{datetime.datetime.now()} --- 🧱 Побудова повної таблиці ОК")

        # 📝 Оновлення в Google Sheets
        # ❗ Не очищаємо заголовки — тільки дані
        # Отримаємо кількість рядків, видалимо старі дані з другого рядка вниз
        num_rows = len(sheet.get_all_values())
        if num_rows > 1:
            sheet.batch_clear([f"A2:Z{num_rows}"])
        values = [list(row) for row in output]
        sheet.update(range_name="A2", values=values)
        # print(f"✅️{datetime.datetime.now()} Запис ОК ")
        result.append(f"✅️{datetime.datetime.now()} -- Запис ОК ")
        print(f"⏩{datetime.datetime.now()} ==== END TO CLOUD ========================================================")
        return result
    except Exception as e:
        print(f"⏩{datetime.datetime.now()} ==== BEGIN TO CLOUD ======================================================")
        return str(e)

    # except Exception as e:
    #     result.append(f" ❗{datetime.datetime.now()} -- ERROR: {str(e)}  ")
    # return result
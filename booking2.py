import fbextract
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta

# Параметри Firebird
fb = fbextract.get_connection()

# Авторизація Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("creds/credentials.json", scope)
client = gspread.authorize(creds)
spreadsheet = client.open_by_key("1P2DWthoBNJ0Q4Xfop76-yc-XZRbgH6DECU6JSmwv3jI")

# Відкриваємо або створюємо лист для другої таблиці
try:
    sheet2 = spreadsheet.worksheet("Бронювання_по_машинах")
except gspread.exceptions.WorksheetNotFound:
    sheet2 = spreadsheet.add_worksheet(title="Бронювання_по_машинах", rows="100", cols="2")

cur = fb.cursor()

# Отримуємо всі машини
cur.execute("SELECT DISTINCT MIL_NUM FROM v_BOOKING")
cars = [row[0] for row in cur.fetchall()]

data = [["Машина", "Дати бронювання"]]

for car in cars:
    # Всі бронювання машини за весь час
    cur.execute("""
        SELECT DATE_, bat_order FROM v_BOOKING 
        WHERE MIL_NUM = ? 
        ORDER BY DATE_
    """, (car,))
    rows = cur.fetchall()

    colors = ["🟢", "🔴", "🟡", "🔵", "🟣"]
    tags = [f"{colors[i % len(colors)]} {r[0].strftime('%d.%m.%Y')} - {r[1]}" for i, r in enumerate(rows)]
    # tag_str = "\n".join(tags) if tags else ""
    tag_str = "".join(tags) if tags else ""
    data.append([car, tag_str])

# Очищаємо лист і оновлюємо
sheet2.clear()
sheet2.update(range_name="A1", values=data)

fb.close()

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta

import fbextract

# 🔐 Параметри Firebird
fb = fbextract.get_connection()

# 🔐 Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("creds/credentials.json", scope)
client = gspread.authorize(creds)
sheet = client.open_by_key("1P2DWthoBNJ0Q4Xfop76-yc-XZRbgH6DECU6JSmwv3jI").sheet1

# 📅 Діапазон на 7 днів
start_date = datetime.today().date()
dates = [start_date + timedelta(days=i) for i in range(7)]
date_headers = [d.strftime('%d.%m') for d in dates]

cur = fb.cursor()

# 🚘 Машини
cur.execute("SELECT DISTINCT MIL_NUM FROM V_BOOKING")
cars = [row[0] for row in cur.fetchall()]

# 📋 Формуємо таблицю
header = ["Машина"] + date_headers
table = [header]

for car in cars:
    row = [car]
    for d in dates:
        cur.execute("""
            SELECT BAT_ORDER FROM V_BOOKING 
            WHERE MIL_NUM = ? AND DATE_ = ?
        """, (car, d))
        people = [r[0] for r in cur.fetchall()]
        colors = ["🟢", "🔴", "🟡", "🔵", "🟣"]
        tags = [f"{colors[i % len(colors)]} {p}" for i, p in enumerate(people)]
        tag = "\n".join(tags) if tags else ""
        row.append(tag)
    table.append(row)

# 🔄 Оновлюємо Google Таблицю
sheet.clear()
sheet.update(range_name="A1", values=table)

fb.close()

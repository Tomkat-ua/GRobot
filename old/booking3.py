import fbextract
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from collections import defaultdict

# 🔹 Підключення до Firebird
con = fbextract.get_connection()
cur = con.cursor()
cur.execute("SELECT mil_num, date_, bat_order FROM v_BOOKING")
rows = cur.fetchall()
con.close()

# 🔹 Побудова словника: бронювання по mil_num
booking_data = defaultdict(dict)
all_dates = set()

for mil_num, date_, order in rows:
    date_str = date_.strftime('%d.%m.%Y')
    booking_data[mil_num][date_str] = order
    all_dates.add(date_str)
    # print(f"{mil_num} {date_} {order}")

sorted_dates = sorted(all_dates, key=lambda d: datetime.strptime(d, '%d.%m.%Y'))
# print(sorted_dates)
#
# 🔹 Підключення до Google Sheets
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('../creds/credentials.json', scope)
client = gspread.authorize(creds)
#
# sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1dUieMDyXKqKM8ijBqeTlq9lJv6ScmWM8t2_ht7NJeWE").worksheet("Авто")
url = 'https://docs.google.com/spreadsheets/d/1DsiSD8rZbwr-rP9K86H2n6h7ijj4IXEO9G4g66dzSb8/edit?gid=0#gid=0'
sheet = client.open_by_url(url).worksheet("Список")

# 🔹 Зчитування існуючих даних
key_col = 1 #стовбець - ключ
existing_data = sheet.get_all_values()
header = existing_data[0]
print(header)
mil_nums = [row[0] for row in existing_data[1:]]
print(mil_nums)
# 🔹 Підготовка нових стовпців
new_cols = sorted_dates
# new_header = header + new_cols
# new_rows = []

# for i, row in enumerate(existing_data[1:], start=1):
#     mil = row[0]
#     bookings = booking_data.get(mil, {})
#     new_cells = [bookings.get(d, '') for d in new_cols]
#     new_rows.append(row + new_cells)
#     print(new_rows)


# Додаємо колонку "ЗАРЕЗЕРВОВАНО"
new_header = header + ["РЕЗЕРВ"] + new_cols
new_rows = []

for i, row in enumerate(existing_data[1:], start=1):
    mil = row[0]
    bookings = booking_data.get(mil, {})
    reserved_flag = 1 if bookings else ""
    new_cells = [reserved_flag] + [bookings.get(d, '') for d in new_cols]
    new_rows.append(row + new_cells)

# # # 🔹 Оновлення таблиці
sheet.clear()
insert_range = f"L2"

# sheet.update(range_name=insert_range, values=updated_values)
sheet.update(range_name=insert_range,values=[new_header] + new_rows)

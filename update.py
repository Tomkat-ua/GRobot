from googleapiclient.discovery import build
from google.oauth2 import service_account
from oauth2client.service_account import ServiceAccountCredentials
import io
from googleapiclient.http import MediaIoBaseDownload


SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
creds = service_account.Credentials.from_service_account_file(
    'credentials.json', scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=creds)

# SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive.readonly']
# creds = ServiceAccountCredentials.from_json_keyfile_name("creds/credentials.json", SCOPES)
#
# gc = gspread.authorize(creds)
# drive = build('drive', 'v3', credentials=creds)

def ie(file_id):
    # Авторизація
    # SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
    # creds = service_account.Credentials.from_service_account_file(
    #     'credentials.json', scopes=SCOPES)
    # drive_service = build('drive', 'v3', credentials=creds)

    # ID Google Sheet файлу (з посилання)
    # file_id = '1dUieMDyXKqKM8ijBqeTlq9lJv6ScmWM8t2_ht7NJeWE'

    # Завантаження у форматі Excel
    request = drive_service.files().export_media(fileId=file_id,
        mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()

    # Збереження локально (не обов’язково, якщо читаєш напряму з BytesIO)
    with open("exported_sheet.xlsx", "wb") as f:
        f.write(fh.getbuffer())


# === Знайти файл .xlsx на Google Drive ===
def find_file_id(file_name):

    query = f"name = '{file_name}' and mimeType = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"
    results = drive.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])
    if not files:
        raise FileNotFoundError(f"❌ Файл '{file_name}' не знайдено на Google Диску.")
    return files[0]['id']

# === Конвертація в Google Sheets ===
def convert_to_gsheet(xlsx_file_id, new_title):
    body = {'name': new_title, 'mimeType': 'application/vnd.google-apps.spreadsheet'}
    converted = drive.files().copy(fileId=xlsx_file_id, body=body).execute()
    print(f"✅ Конвертовано у Google Sheets: {converted['id']}")
    return converted['id']

# === Масове оновлення sync_status у колонці E ===
def batch_update_sync_status(sheet_id, column_letter='E'):
    sheet = gc.open_by_key(sheet_id).sheet1
    records = sheet.get_all_records()
    row_count = len(records)
    print(f"🔍 Обробка {row_count} рядків...")

    cell_range = f'{column_letter}2:{column_letter}{row_count+1}'
    cells = sheet.range(cell_range)

    for i, cell in enumerate(cells):
        value = str(records[i].get('sync_status', '')).strip().lower()
        if value != 'ок':
            cell.value = "ок"

    sheet.update_cells(cells)
    print("✅ Статуси оновлено без перевищення квоти.")

# === Запуск ===
if __name__ == "__main__":
    try:
        xlsx_filename = "Автомобілі_1ЄБ.xlsx"
        sheet_title = "Оброблений"
        xlsx_id = find_file_id(xlsx_filename)
        print(xlsx_id)
        ie(xlsx_id)
        # sheet_id = convert_to_gsheet(xlsx_id, sheet_title)
        # print(sheet_id)
        # batch_update_sync_status(sheet_id)
    except Exception as e:
        print("❌ Помилка:", e)

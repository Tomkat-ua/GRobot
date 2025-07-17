from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

# Авторизація
creds = Credentials.from_service_account_file("credentials.json", scopes=["https://www.googleapis.com/auth/drive"])
drive_service = build("drive", "v3", credentials=creds)

file_id = "your_file_id"
old_folder_id = "old_folder_id"
new_folder_id = "new_folder_id"

# Отримуємо поточних батьків
file = drive_service.files().get(fileId=file_id, fields='parents').execute()
previous_parents = ",".join(file.get('parents'))

# Переміщення файлу
drive_service.files().update(
    fileId=file_id,
    addParents=new_folder_id,
    removeParents=previous_parents,
    fields='id, parents'
).execute()

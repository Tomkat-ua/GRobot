from googleapiclient.discovery import build
from google.oauth2 import service_account

# Авторизація
SCOPES = ['https://www.googleapis.com/auth/drive']
creds = service_account.Credentials.from_service_account_file(
    '../creds/credentials.json', scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=creds)

# Вказати ID файлу для видалення
file_id = '1fQPZ_vjdsCaCqJXoSm63e_5eMCKoTTyp'  # заміни на реальний ID

# Видалення файлу
drive_service.files().delete(fileId=file_id).execute()
print("Файл видалено.")

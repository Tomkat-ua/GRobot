import config
from config import arc_folder_id as new_folder_id

def move_file(file_id):

    drive_service = config.get_drive_service()
    # Отримуємо поточних батьків
    file =  drive_service.files().get(fileId=file_id, fields='parents').execute()
    previous_parents = ",".join(file.get('parents'))

    # Переміщення файлу
    drive_service.files().update(
        fileId=file_id,
        addParents=new_folder_id,
        removeParents=previous_parents,
        fields='id, parents'
    ).execute()


# move_file()
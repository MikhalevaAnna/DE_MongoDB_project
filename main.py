import pymongo.errors
from pymongo import MongoClient
import json
from datetime import datetime, timedelta

current_date = datetime.now()

try:
    client = MongoClient("mongodb://localhost:27017/")
    db = client["my_database"]
    collection = db["user_events"]
    archived_collection = db['archived_users']
except pymongo.errors.ConnectionFailure:
    print("Не удалось подключиться к MongoDB!")
except Exception as e:
    print(f"Ошибка при подключении к MongoDB: {e}")


def archive_inactive_users(registration_days: int=30, inactive_days: int=14) -> dict:
    """
    Архивирует ID неактивных пользователей и формирует по ним информацию для отчета
    """
    registration_threshold = current_date - timedelta(days=registration_days)
    activity_threshold = current_date - timedelta(days=inactive_days)
    # Формируется запрос для поиска неактивных пользователей
    query = {
        "event_time": {"$lt": activity_threshold},
        "user_info.registration_date": {"$lt": registration_threshold}
    }
    # Находятся пользователи для архивации
    users_to_archive = list(collection.find(query))
    # Пользователи архивируются
    if users_to_archive:
        archived_collection.insert_many(users_to_archive)
        # Неактивные пользователи удаляются из основной коллекции
        collection.delete_many(query)
    report = {
         "date": current_date.strftime('%Y-%m-%d'),
         "archived_users_count": len(users_to_archive),
         "archived_user_ids": list(map(lambda user: user["user_id"], users_to_archive))
    }
    return report


def save_report_to_file(report: dict) -> None:
    """
    Сохраняет отчет в JSON файл
    """
    report_filename = f"archive_report_{current_date.strftime('%Y-%m-%d')}.json"
    with open(report_filename, 'w', encoding='utf-8') as report_out:
        json.dump(report, report_out, indent=2, ensure_ascii=False)
    print(f"Всего перемещено в архив {len(report['archived_user_ids'])} неактивных пользователей.")
    print(f"Отчет сохранен в файл: {report_filename}")


if __name__ == "__main__":
    data = archive_inactive_users()
    save_report_to_file(data)

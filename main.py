import pymongo.errors
from pymongo import MongoClient
import json
from datetime import datetime, timedelta


try:
    client = MongoClient("mongodb://localhost:27017/")
    db = client["my_database"]
    user_events = db["user_events"]
    archived_users = db['archived_users']
except pymongo.errors.ConnectionFailure:
    print("Не удалось подключиться к MongoDB!")
except Exception as e:
    print(f"Ошибка при подключении к MongoDB: {e}")


def archive_inactive_users(current_date: datetime, registration_days: int = 30, inactive_days: int = 14) -> dict:
    """
    Архивирует неактивных пользователей и формирует по ним информацию для отчета
    """
    registration_threshold = current_date - timedelta(days = registration_days)
    activity_threshold = current_date - timedelta(days = inactive_days)
    # Находятся ID пользователей, у которых последнее событие было более 14 дней назад и
    # регистрация больше 30 дней назад
    query  = [
        {
            "$group": {
                "_id": "$user_id",
                "last_activity": {"$max": "$event_time"},
                "registration_date": {"$first": "$user_info.registration_date"}
            }
        },
        {
            "$match": {
                "last_activity": {"$lt": activity_threshold},
                "registration_date": {"$lt": registration_threshold}
            }
        },
        {
            "$sort": {
                "_id": 1
            }
        }
    ]

    users_to_archive = list(user_events.aggregate(query))
    if not users_to_archive:
        return {
            "date": current_date.strftime('%Y-%m-%d'),
            "archived_users_count": 0,
            "archived_user_ids": []
        }
    try:
        archived_user_ids = []
        for user in users_to_archive:
            archived_user = user_events.find_one({"user_id": user['_id']})
            if archived_user:
                # Документы добавляются в архив
                 archived_users.insert_one(archived_user)
                # Добавляются в список user_id для отчета
                 archived_user_ids.append(user['_id'])
        # Удаляются из основной коллекции
        user_events.delete_many({"user_id": {"$in": archived_user_ids}})
    except Exception as e:
        print(f"Ошибка при архивации: {e}")
        return {"error": f"Archive failed: {e}"}

    report = {
        "date": current_date.strftime('%Y-%m-%d'),
        "archived_users_count": len(archived_user_ids),
        "archived_user_ids": archived_user_ids
    }
    return report


def save_report_to_file(current_date: datetime, report: dict) -> None:
    """
    Сохраняет отчет в JSON файл
    """
    try:
        report_filename = f"archive_report_{current_date.strftime('%Y-%m-%d')}.json"

        with open(report_filename, 'w', encoding = 'utf-8') as report_out:
            json.dump(report, report_out, indent = 2, ensure_ascii = False)

        print(f"Всего перемещено в архив {len(report['archived_user_ids'])} неактивных пользователей.")
        print(f"Отчет сохранен в файл: {report_filename}")

    except PermissionError:
        print(f"Ошибка: Нет прав для записи в файл {report_filename}")
    except IOError as e:
        print(f"Ошибка ввода-вывода при записи файла: {e}")
    except Exception as e:
        print(f"Неожиданная ошибка при сохранении отчета: {e}")


if __name__ == "__main__":
    current_date = datetime.now()
    report_data = archive_inactive_users(current_date)
    if 'error' not in report_data:
        save_report_to_file(current_date, report_data)
    else:
        print(f"Ошибка архивации: {report_data['error']}")

from pymongo import MongoClient
from datetime import datetime, timedelta
import json


def main() -> None:
    # Подключение к базе данных
    client = MongoClient("mongodb://localhost:27017/")
    db = client["my_database"]
    user_events = db["user_events"]
    archived_users = db["archived_users"]

    # Получение текущей даты
    current_date = datetime.now()

    # Инициализация отчета за день
    daily_report = {
        "date": current_date.strftime("%Y-%m-%d"),
        "archived_users_count": 0,
        "archived_users_ids": []
    }

    for doc in user_events.find():
        # Проверяем дату регистрации пользователя.
        # Если количество дней с момента регистрации больше 30 продолжаем проверку.
        try:
            if (current_date - doc["user_info"]["registration_date"]) > timedelta(days=30):
                # Проверяем, была ли активность за последние 14 дней
                recent_login = user_events.find_one({"user_id": doc["user_id"],
                                                     "event_time": { "$gt": datetime.now() - timedelta(days=14)}})
                if not recent_login:
                    # Если активности не было, заполням отчет и архивируем пользователя
                    daily_report["archived_users_count"] += 1
                    daily_report["archived_users_ids"].append(doc["user_id"])
                    archived_users.insert_one(doc)
                    user_events.delete_one({"_id": doc["_id"]})
        except KeyError as k_err:
            print(k_err)
        except Exception as err:
            print(err)

    # Выгружаем отчет в json файл
    try:
        with open(f"{current_date.strftime("%Y-%m-%d")}.json", "w", encoding="utf-8") as f:
            json.dump(daily_report, f, ensure_ascii=False, indent=4)
    except FileNotFoundError as f_err:
        print(f_err)
    except Exception as err:
        print(err)

if __name__ == "__main__":
    main()

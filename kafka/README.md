# Миграция данных из Postgres в Clickhouse
## Структура
***
В основной базе данных PostgreSQL хранится таблица user_logins.
В таблице user_logins содержатся события, которые производили пользователи в системе.
***
В аналитической базе данных Clickhouse содержится таблица user_logins
в которую переносятся события из основной базы данных для последующей обработки.
## Пайплайн
1. Исходные данные должны быть взяты из таблицы user_logins основной базы данных
2. Python скрипт producer_from_kafka.py забирает данные из таблицы user_logins и загружает в kafka. Скрипт запускается командой: ```python producer_from_kafka.py```
3. Python скрипт consumer_to_clickhouse.py забирает данные из kafka и загружает в таблицу user_logins в аналитической базе данных. Скрипт запускается командой: ```python consumer_to_clickhouse.py```
---
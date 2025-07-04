from kafka import KafkaConsumer
import json
import clickhouse_connect


consumer = KafkaConsumer(
    'user_logins',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='user_logins_consumer'
)

client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='user',
    password='strongpassword'
)

client.command(
    """
    create table if not exists user_logins (
        username String,
        event_type String,
        date_event DateTime,
    ) engine = MergeTree()
    order by date_event
    """
)
for message in consumer:
    data = message.value
    print("Received message:", data)
    client.command(
        f"""
        insert into user_logins (username, event_type, date_event)
            values ('{data['username']}', '{data['type_event']}', toDateTime({data['date_time']}))
        """
    )
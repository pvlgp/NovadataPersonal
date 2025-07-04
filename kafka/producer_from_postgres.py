from kafka import KafkaProducer
import psycopg2
import json
import time


producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

conn = psycopg2.connect(
    dbname='test_db',
    user='admin',
    password='admin',
    host='localhost',
    port='5432'
)

cursor = conn.cursor()

cursor.execute(
    """
    select 
        id,
        username,
        type_event,
        extract(epoch from date_time)
    from user_logins
    where sent_to_kafka is false 
    """
)
rows = cursor.fetchall()

for row in rows:
    data = {
        'username': row[1],
        'type_event': row[2],
        'date_time': float(row[3]),
    }
    cursor.execute(
        """
        update user_logins set sent_to_kafka = true where id = %s
        """,
        (row[0],)
    )
    conn.commit()
    producer.send('user_logins', data)
    print('Sent:', data)
    time.sleep(0.5)

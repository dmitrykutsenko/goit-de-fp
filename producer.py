import json
import pandas as pd
from sqlalchemy import create_engine
from kafka import KafkaProducer

# --------------------------------------------------
# Етап 3: зчитування даних з MySQL таблиці athlete_event_results
# --------------------------------------------------

MYSQL_USER = "airflow"
MYSQL_PASSWORD = "airflow"
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DB = "olympic_dataset"

engine = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
)

query = """
SELECT *
FROM athlete_event_results
"""

df = pd.read_sql(query, engine)

print(f"Зчитано рядків: {len(df)}")

# --------------------------------------------------
# Етап 3: запис даних у Kafka topic athlete_event_results
# --------------------------------------------------

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

for _, row in df.iterrows():
    producer.send(
        "athlete_event_results",
        row.to_dict()
    )

producer.flush()

print("Відправлення в Kafka завершено")

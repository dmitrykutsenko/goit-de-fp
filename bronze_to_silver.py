import re
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


# Етап 2: функція очищення тексту
def clean_text(text):
    return re.sub(
        r'[^a-zA-Z0-9,.\\"\']',
        '',
        str(text)
    )


clean_text_udf = udf(clean_text, StringType())


def main(table_name):

    spark = (
        SparkSession.builder
        .appName(f"BronzeToSilver_{table_name}")
        .getOrCreate()
    )

    # Етап 2: зчитування bronze таблиці
    df = spark.read.parquet(f"bronze/{table_name}")

    # Етап 2: чистка текстових колонок
    for field in df.schema.fields:

        if field.dataType.typeName() == "string":

            df = df.withColumn(
                field.name,
                clean_text_udf(col(field.name))
            )

    # Етап 2: дедублікація
    df = df.dropDuplicates()

    # Вивід датафрейму для логів Airflow
    print(f"\n===== BRONZE -> SILVER : {table_name} =====")
    df.show(20, truncate=False)

    # Етап 2: запис у silver layer
    (
        df.write
        .mode("overwrite")
        .parquet(f"silver/{table_name}")
    )

    print(f"Saved to silver/{table_name}")

    spark.stop()


if __name__ == "__main__":

    if len(sys.argv) != 2:
        raise Exception(
            "Usage: spark-submit bronze_to_silver.py <table_name>"
        )

    main(sys.argv[1])
    
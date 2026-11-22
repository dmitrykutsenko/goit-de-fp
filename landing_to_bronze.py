import sys

from pyspark.sql import SparkSession


def main(table_name):

    spark = (
        SparkSession.builder
        .appName(f"LandingToBronze_{table_name}")
        .getOrCreate()
    )

    # Етап 1: читання csv із landing layer
    df = (
        spark.read
        .option("header", "true")
        .option("multiLine", "true")
        .option("escape", "\"")
        .option("inferSchema", "true")
        #.csv(f"data/{table_name}.csv")
        .csv(f"/opt/airflow/project/data/{table_name}.csv")
    )

    # Вивід датафрейму для логів Airflow
    print(f"\n===== LANDING -> BRONZE : {table_name} =====")
    df.show(20, truncate=False)

    # Етап 1: запис у bronze layer у форматі parquet
    (
        df.write
        .mode("overwrite")
        .parquet(f"bronze/{table_name}")
    )

    print(f"Saved to bronze/{table_name}")

    spark.stop()


if __name__ == "__main__":

    if len(sys.argv) != 2:
        raise Exception(
            "Usage: spark-submit landing_to_bronze.py <table_name>"
        )

    main(sys.argv[1])
    
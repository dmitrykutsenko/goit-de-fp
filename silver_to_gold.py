from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    current_timestamp
)


def main():

    spark = (
        SparkSession.builder
        .appName("SilverToGold")
        .getOrCreate()
    )

    # Етап 3: зчитування таблиці silver/athlete_bio
    athlete_bio_df = spark.read.parquet(
        "silver/athlete_bio"
    )

    # Етап 3: зчитування таблиці silver/athlete_event_results
    athlete_event_results_df = spark.read.parquet(
        "silver/athlete_event_results"
    )

    # Етап 3: приведення типів для математичних операцій
    athlete_bio_df = (
        athlete_bio_df
        .withColumn(
            "height",
            col("height").cast("double")
        )
        .withColumn(
            "weight",
            col("weight").cast("double")
        )
    )

    # Етап 3: відфільтрувати записи,
    # де зріст/вага відсутні
    athlete_bio_df = athlete_bio_df.filter(
        col("height").isNotNull()
    ).filter(
        col("weight").isNotNull()
    )

    # Етап 3: join по athlete_id
    #joined_df = athlete_event_results_df.join(
    #    athlete_bio_df,
    #    on="athlete_id",
    #    how="inner"
    #)
    ###
    # Етап 3: join по athlete_id
    joined_df = athlete_event_results_df.alias("event").join(
        athlete_bio_df.alias("bio"),
        on="athlete_id",
        how="inner"
    )

    result_df = joined_df.select(
        col("event.sport").alias("sport"),
        col("event.medal").alias("medal"),
        col("bio.sex").alias("sex"),
        col("event.country_noc").alias("country_noc"),
        col("bio.height").alias("height"),
        col("bio.weight").alias("weight")
    )
    ###

    # Етап 3: агрегація
    gold_df = (
    result_df
        .groupBy(
            "sport",
            "medal",
            "sex",
            "country_noc"
        )
        .agg(
            avg("height").alias("avg_height"),
            avg("weight").alias("avg_weight")
        )
        .withColumn(
            "timestamp",
            current_timestamp()
        )
    )

    # Виведення результату для логів Airflow
    print("\n===== SILVER -> GOLD =====")
    gold_df.show(20, truncate=False)

    # Етап 3: запис у gold layer
    (
        gold_df.write
        .mode("overwrite")
        .parquet("gold/avg_stats")
    )

    print("Saved to gold/avg_stats")

    spark.stop()


if __name__ == "__main__":
    main()

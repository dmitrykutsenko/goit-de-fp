from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    avg,
    current_timestamp,
    to_json,
    struct
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType
)

# ==================================================
# Налаштування
# ==================================================

MYSQL_URL = "jdbc:mysql://localhost:3306/olympic_dataset"
MYSQL_USER = "airflow"
MYSQL_PASSWORD = "airflow"

INPUT_TOPIC = "athlete_event_results"
OUTPUT_TOPIC = "athlete_analytics"

# ==================================================
# Spark Session
# ==================================================

spark = (
    SparkSession.builder
    .appName("OlympicStreamingPipeline")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ==================================================
# Етап 1: зчитування athlete_bio з MySQL
# ==================================================

athlete_bio_df = (
    spark.read
    .format("jdbc")
    .option("url", MYSQL_URL)
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "athlete_bio")
    .option("user", MYSQL_USER)
    .option("password", MYSQL_PASSWORD)
    .load()
)

# ==================================================
# Етап 2: фільтрація порожніх значень
# ==================================================

athlete_bio_df = (
    athlete_bio_df
    .filter(col("height").isNotNull())
    .filter(col("weight").isNotNull())
)

# ==================================================
# Етап 3: схема повідомлень Kafka
# ==================================================

event_schema = StructType([
    StructField("edition", StringType()),
    StructField("edition_id", IntegerType()),
    StructField("country_noc", StringType()),
    StructField("sport", StringType()),
    StructField("event", StringType()),
    StructField("result_id", LongType()),
    StructField("athlete", StringType()),
    StructField("athlete_id", LongType()),
    StructField("pos", StringType()),
    StructField("medal", StringType()),
    StructField("isTeamSport", StringType())
])

# ==================================================
# Етап 3: читання Kafka
# ==================================================

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

events_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING)")
    .select(
        from_json(
            col("value"),
            event_schema
        ).alias("data")
    )
    .select("data.*")
)

# ==================================================
# Етап 4: JOIN
# ==================================================

joined_df = (
    events_df.alias("e")
    .join(
        athlete_bio_df.alias("b"),
        on="athlete_id",
        how="inner"
    )
    .select(
        col("athlete_id"),
        col("sport"),
        col("medal"),
        col("sex"),
        col("height"),
        col("weight"),
        col("b.country_noc").alias("country_noc")
    )
)

# ==================================================
# Етап 5: агрегація
# ==================================================

result_df = (
    joined_df
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
        "calculation_time",
        current_timestamp()
    )
)

# ==================================================
# Етап 6: foreachBatch
# ==================================================

def foreach_batch_function(batch_df, batch_id):

    print(f"\n=== Batch {batch_id} ===\n")

    if batch_df.isEmpty():
        return

    batch_df.show(
        20,
        truncate=False
    )

    # ----------------------------------------------
    # Етап 6.a): запис у Kafka
    # ----------------------------------------------

    kafka_output_df = (
        batch_df.select(
            to_json(
                struct("*")
            ).alias("value")
        )
    )

    (
        kafka_output_df
        .write
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            "localhost:9092"
        )
        .option(
            "topic",
            OUTPUT_TOPIC
        )
        .save()
    )

    # ----------------------------------------------
    # Етап 6.b): запис у MySQL
    # ----------------------------------------------

    (
        batch_df.write
        .format("jdbc")
        .option("url", MYSQL_URL)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", "athlete_analytics")
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .mode("append")
        .save()
    )

# ==================================================
# Запуск стріму
# ==================================================

query = (
    result_df.writeStream
    .outputMode("complete")
    .foreachBatch(foreach_batch_function)
    .start()
)

query.awaitTermination()

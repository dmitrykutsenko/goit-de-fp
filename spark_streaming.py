from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    avg,
    current_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType
)

# ==================================================
# Етап 1: зчитування біологічних даних з MySQL
# ==================================================

MYSQL_USER = "airflow"
MYSQL_PASSWORD = "airflow"
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DB = "olympic_dataset"

JDBC_URL = (
    f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
)

spark = (
    SparkSession.builder
    .appName("OlympicStreamingPipeline")
    #.config(
    #    "spark.jars",
    #    "mysql-connector-j-8.0.32.jar"
    #)
    .config(
        "spark.jars",
        "/mnt/c/Goit/goit-de-fp/mysql-connector-j-8.4.0.jar"
    )
    .getOrCreate()
)

athlete_bio_df = (
    spark.read
    .format("jdbc")
    .option("url", JDBC_URL)
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "athlete_bio")
    .option("user", MYSQL_USER)
    .option("password", MYSQL_PASSWORD)
    .load()
)

# ==================================================
# Етап 2: фільтрація порожніх значень height/weight
# ==================================================

athlete_bio_df = athlete_bio_df.filter(
    col("height").isNotNull()
)

athlete_bio_df = athlete_bio_df.filter(
    col("weight").isNotNull()
)

# ==================================================
# Етап 3: зчитування даних з Kafka-topic
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

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "athlete_event_results")
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
# Етап 4: об'єднання даних через athlete_id
# ==================================================

joined_df = events_df.join(
    athlete_bio_df,
    on="athlete_id",
    how="inner"
)

# ==================================================
# Етап 5: розрахунок середнього зросту та ваги
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
# Етап 6: запис результатів у Kafka та MySQL
# ==================================================

def foreach_batch_function(batch_df, batch_id):

    print(f"Обробка batch_id={batch_id}")

    # ----------------------------------------------
    # Етап 6.а): запис у вихідний Kafka-топік
    # ----------------------------------------------

    kafka_output_df = (
        batch_df.selectExpr(
            "to_json(struct(*)) AS value"
        )
    )

    (
        kafka_output_df.write
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            "localhost:9092"
        )
        .option(
            "topic",
            "athlete_analytics"
        )
        .save()
    )

    # ----------------------------------------------
    # Етап 6.b): запис у MySQL
    # ----------------------------------------------

    (
        batch_df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option(
            "driver",
            "com.mysql.cj.jdbc.Driver"
        )
        .option(
            "dbtable",
            "athlete_analytics"
        )
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
    ###
    debug_query = (
        result_df.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", False)
        .start()
    )
    ###
    .foreachBatch(foreach_batch_function)
    .start()
)

query.awaitTermination()

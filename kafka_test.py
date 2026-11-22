from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("KafkaTest")
    .getOrCreate()
)

df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "localhost:9092"
    )
    .option(
        "subscribe",
        "athlete_event_results"
    )
    .option(
        "startingOffsets",
        "earliest"
    )
    .load()
)

#result = (
#    df.selectExpr(
#        "CAST(value AS STRING)"
#    )
#)
###
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType
)

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

result = (
    df
    .selectExpr("CAST(value AS STRING)")
    .select(
        from_json(
            col("value"),
            event_schema
        ).alias("data")
    )
    .select("data.*")
)

result.printSchema()
###

query = (
    result.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
)

query.awaitTermination()

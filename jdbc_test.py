from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("JDBCTest")
    #.config(
    #    "spark.jars",
    #    "/mnt/c/Goit/goit-de-fp/mysql-connector-j-8.4.0.jar"
    #)
    .getOrCreate()
)

df = (
    spark.read
    .format("jdbc")
    .option(
        "url",
        "jdbc:mysql://localhost:3306/olympic_dataset"
    )
    .option(
        "driver",
        "com.mysql.cj.jdbc.Driver"
    )
    .option(
        "dbtable",
        "athlete_bio"
    )
    .option(
        "user",
        "airflow"
    )
    .option(
        "password",
        "airflow"
    )
    .load()
)

print("Успішне підключення до MySQL")

df.printSchema()

df.show(5, truncate=False)

spark.stop()

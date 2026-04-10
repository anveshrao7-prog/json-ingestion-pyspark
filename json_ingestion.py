from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("JSON Ingestion Project") \
    .getOrCreate()

df = spark.read.option("multiline", "true") \
    .json("data/emp3.json")

df.show(5)
df.printSchema()

df.write.mode("overWrite").parquet("output/json_parquet")
print("Task completed successfully!")


from pyspark.sql.functions import col, explode

flat_df = df.select(
    col("__PublishDate"),
    col("events.beaconType"),
    col("events.beaconVersion"),
    col("events.client"),

    # nested struct
    col("events.data.displayAd.amazonA9HB"),
    col("events.data.displayAd.inView"),

    # milestones
    col("events.data.milestones.adRequested"),
    col("events.data.milestones.amazonA9BidsReceived"),

    # headers (array → explode)
    explode(col("headers.Accept")).alias("Accept_header")
)

flat_df.show(5)
flat_df.printSchema()

flat_df.write.mode("overwrite").parquet("output/json_flattened")

# feature_1.0 update


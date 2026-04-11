from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("JSON Ingestion Project") \
    .getOrCreate()

schema = spark.read.json("data/schema.json").schema
df=spark.read.schema(schema).json("data/emp3.json")
#df.show()

flat_df = df.select(
    col("__PublishDate"),
    col("events.beaconType"),
    col("events.beaconVersion"),
    col("events.client"),
    col("events.data.displayAd.amazonA9HB"),
    col("events.data.displayAd.inView"),
    col("events.data.milestones.adRequested"),
    col("events.data.milestones.amazonA9BidsReceived"),)

flat_df.show(5,False)

#flat_df.write.mode("overwrite").parquet("output/json_flattened")



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

#flatten logic above

from pyspark.sql.functions import col, count, when, countDistinct, lit
cols = [
    "__PublishDate","beaconType","beaconVersion","client","amazonA9HB","inView", "adRequested","amazonA9BidsReceived"]
total_count = flat_df.count()
dfs = []
for c in cols:
    df = flat_df.select(
        lit(c).alias("col_name"),
        lit(total_count).alias("total_record_count"),
        count(when(col(c).isNull(), 1)).alias("null_record_count"),
        lit(total_count).alias("source_record_count"),
        (lit(total_count) - countDistinct(col(c))).alias("duplicate_count")
    )
    dfs.append(df)

profile_df = dfs[0]
for d in dfs[1:]:
    profile_df = profile_df.unionByName(d)

profile_df.show()

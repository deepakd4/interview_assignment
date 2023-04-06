from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

spark = SparkSession.builder.appName("read_csv_files").getOrCreate()
spark.sparkContext.setLogLevel('ERROR')


folder_path = "C:/Users/deepa.LAPTOP-K49ED5D2/Work/Assignment/archive"

# from google.cloud import storage
# storage_client = storage.Client()
# blobs = storage_client.list_blobs(bucket_name)
# csv_files = [f.name for f in blobs if f.endswith(".csv")]


csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]

dfs = []

for file_name in csv_files:
    country_code = file_name[:2]
    df = spark.read.format("csv") \
        .option("encoding", "utf-8") \
        .option("multiline", True)\
        .option("header", True) \
        .load(os.path.join(folder_path, file_name))
    df = df.withColumn("country", F.lit(country_code))
    df = df.withColumn("publish_year", F.year(F.col("publish_time")))
    df = df.withColumn("publish_month", F.month(F.col("publish_time")))
    dfs.append(df)


new_df = None
for index, df in enumerate(dfs):
    if index == 0:
        new_df = df
    else:
        new_df = new_df.unionByName(df)

new_df.cache()

history_final_path = 'C:/Users/deepa.LAPTOP-K49ED5D2/Work/Assignment/final'

if len(os.listdir(history_final_path)) == 0:
    schema = new_df.schema
    history_df = spark.read.format("parquet").option("encoding", "utf-8").schema(schema).load(history_final_path)
else:
    history_df = spark.read.format("parquet").option("encoding", "utf-8").option('inferSchema', True).load(history_final_path)

history_df.cache()
new_df.createOrReplaceTempView('New')
history_df.createOrReplaceTempView('History')
updated_history_df = spark.sql("select * from History where video_id in (select video_id from History minus select video_id from New)").unionByName(new_df)
updated_history_df.cache()
updated_history_df.first()
new_df.unpersist()
history_df.unpersist()
updated_history_df.repartition(1).write.partitionBy('publish_year', 'publish_month', 'category_id', 'country') \
            .parquet(history_final_path, mode='overwrite')



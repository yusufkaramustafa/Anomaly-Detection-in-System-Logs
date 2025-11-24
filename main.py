from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HDFS_Anomaly_Detection") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

print("Spark session created!")

log_path = "data/HDFS.log"

df_logs = spark.read.text(log_path)

df_logs.show(20, truncate=False)
df_logs.printSchema()
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, when, lit, regexp_replace
from functools import reduce

spark = SparkSession.builder \
    .appName("HDFS_Anomaly_Detection") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

print("Spark session created!")

log_path = "data/HDFS.log"
template_path = "data/HDFS_templates.csv"

df_logs = spark.read.text(log_path)
df_templates = spark.read.csv(template_path, header=True)

# Convert templates into regex patterns
df_templates = df_templates.withColumn(
    "Regex",
    regexp_replace(col("EventTemplate"), "<\\*>", "(.*)")
)

# Start with EventId = null
df_parsed = df_logs.withColumn("EventId", col("value"))

# Apply templates
for row in df_templates.collect():
    event_id = row["EventId"]
    pattern = row["Regex"]

    df_parsed = df_parsed.withColumn(
        "EventId",
        when(col("value").rlike(pattern), event_id).otherwise(col("EventId"))
    )

for row in df_templates.collect():
    eid = row["EventId"]
    pattern = row["Regex"]

    df_parsed = df_parsed.withColumn(
        "Parameter1",
        when(col("EventId") == eid, regexp_extract(col("value"), pattern, 1))
    ).withColumn(
        "Parameter2",
        when(col("EventId") == eid, regexp_extract(col("value"), pattern, 2))
    ).withColumn(
        "Parameter3",
        when(col("EventId") == eid, regexp_extract(col("value"), pattern, 3))
    )


df_parsed.show(20, truncate=False)
df_parsed.printSchema()
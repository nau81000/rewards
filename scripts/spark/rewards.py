from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("RedpandaToDelta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# Lecture en streaming depuis Redpanda/Kafka
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "redpanda:9092")
    .option("subscribe", "sport_data.public.activities")
    .option("startingOffsets", "earliest")
    .load()
)

# Transformation : messages = clé/valeur (bytes) → texte
df_parsed = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

print('***', df_parsed)
# Écriture continue vers Delta Lake
query = (
    df_parsed.writeStream
    .format("delta")
    .option("checkpointLocation", "/opt/spark-data/checkpoints/postgres_changes")
    .outputMode("append")
    .start("/opt/spark-data/delta/postgres_changes")
)

query.awaitTermination()
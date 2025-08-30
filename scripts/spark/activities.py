from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType
from pyspark.sql.functions import col, from_json
from dotenv import load_dotenv
from os import getenv
import time

# Load environment variables
load_dotenv()
POSTGRES_DB_NAME = getenv('POSTGRES_DB_NAME')
POSTGRES_ADMIN_USER = getenv('POSTGRES_ADMIN_USER')
POSTGRES_ADMIN_PWD = getenv('POSTGRES_ADMIN_PWD')
POSTGRES_DB_HOST = getenv('POSTGRES_DB_HOST')

# Create Spark session
spark = (
    SparkSession.builder
    .appName("RedpandaActivitiesToDelta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.10.2,org.apache.hadoop:hadoop-client:2.10.2")
    .getOrCreate()
)

# Read stream from Redpanda
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "redpanda:9092")
    .option("subscribe", "sport_data.public.activities")
    .option("startingOffsets", "earliest")
    .load()
)

# Step 1: Extract JSON string from Kafka value
json_df = raw_df.selectExpr("CAST(value AS STRING) as json")

# Step 2: Extract "payload" first
activity_schema = StructType([
    StructField("before", StructType([
        StructField("id", IntegerType()),
        StructField("id_employee", IntegerType()),
        StructField("id_sport", IntegerType()),
        StructField("distance", DoubleType()),
        StructField("start_date", LongType()),
        StructField("end_date", LongType()),
        StructField("comment", StringType())
    ]), nullable=True),
    StructField("after", StructType([
        StructField("id", IntegerType()),
        StructField("id_employee", IntegerType()),
        StructField("id_sport", IntegerType()),
        StructField("distance", DoubleType()),
        StructField("start_date", LongType()),
        StructField("end_date", LongType()),
        StructField("comment", StringType())
    ]), nullable=True),
    StructField("op", StringType(), nullable=True),
    StructField("ts_ms", LongType(), nullable=True)
])
payload_schema = StructType([
    StructField("before", activity_schema["before"].dataType, True),
    StructField("after", activity_schema["after"].dataType, True),
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True)
])
outer_schema = StructType([
    StructField("schema", StringType(), True),   # we ignore this
    StructField("payload", payload_schema, True)
])

# Step 3: Parse into structured fields
parsed_df = (
    json_df.withColumn("data", from_json(col("json"), outer_schema))
           .select("data.payload.*")  # only payload
)
# Step 4: Access "after" fields directly
activities_df = parsed_df.select("after.*")

# Step 5: Get employees table
while True:
    try:
        sports_df = (
            spark.read
            .format("jdbc")
            .option("url", f"jdbc:postgresql://{POSTGRES_DB_HOST}:5432/{POSTGRES_DB_NAME}")
            .option("dbtable", "public.sports")
            .option("user", POSTGRES_ADMIN_USER)
            .option("password", POSTGRES_ADMIN_PWD)
            .option("driver", "org.postgresql.Driver") 
            .load()
        )
        break
    except Exception:
        # Pass until it works
        time.sleep(1)

# Step 6 : join data frame
joined_df = activities_df.join(
    sports_df,
    activities_df.id_sport == sports_df.id,
    "left"
).select(
    activities_df.id,
    activities_df.id_employee,
    sports_df.name,
    activities_df.distance,
    activities_df.start_date,
    activities_df.end_date,
    activities_df.comment
)

storage = "s3a://databricks-b8znwaytziddgaqktexft7-cloud-storage-bucket/sport-data"
query = (
    joined_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", storage + "/checkpoints/activities")
    .option("mergeSchema", "true")
    .start(storage + "/delta/activities")
)

query.awaitTermination()

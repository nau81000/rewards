from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType
from pyspark.sql.functions import col, from_json
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv
from datetime import datetime
from zoneinfo import ZoneInfo
from babel.dates import format_datetime
from os import getenv
import psycopg2 as pg

# Load environment variables
load_dotenv()
SLACK_TOKEN = getenv("SLACK_TOKEN")
SLACK_CHANNEL = getenv("SLACK_CHANNEL")
SLACK_USERNAME = getenv("SLACK_USERNAME")
# Connect to DB
config = {
    'host': getenv("POSTGRES_DB_HOST"),
    'dbname': getenv("POSTGRES_DB_NAME"), 
    'user': getenv("POSTGRES_ADMIN_USER"), 
    'password': getenv("POSTGRES_ADMIN_PWD"),
    'port': 5432
}
conn = pg.connect(**config)
cursor = conn.cursor()
# Connect to Slack
slack_client = WebClient(token=SLACK_TOKEN)

spark = (
    SparkSession.builder
    .appName("RedpandaToDelta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# Lecture en streaming depuis Redpanda/Kafka
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "redpanda:9092")
    .option("subscribe", "sport_data.public.activities")
    .option("startingOffsets", "earliest")
    .load()
)

# === Query A: Write to Delta Lake ===
# delta_query = (
#     raw_df.writeStream
#     .format("delta")
#     .outputMode("append")
#     .option("checkpointLocation", "/opt/spark-checkpoints/activities_checkpoint")
#     .start("/opt/delta/activities")
# )

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

def send_to_slack(batch_df, batch_id):
    for row in batch_df.collect():
        try:
            # Difference in microseconds
            delta_us = row['end_date'] - row['start_date']
            # Convert in hours
            delta_hours = delta_us / 1_000_000 / 3600
            # Get the activity date in french
            start_date = datetime.fromtimestamp(row['start_date'] / 1_000_000, tz=ZoneInfo("Europe/Paris"))
            formatted_date = format_datetime(start_date, "EEEE d MMMM yyyy", locale="fr_FR")
            # Get employee first and last names
            cursor.execute(f"SELECT first_name, last_name FROM employees WHERE id_employee={row['id_employee']}")
            result = cursor.fetchone()
            first_name = result[0]
            last_name = result[1]
            # Get sport name
            cursor.execute(f"SELECT sport FROM sports WHERE id_sport={row['id_sport']}")
            sport = cursor.fetchone()[0]
            # Building message
            text_msg= f"Bravo {first_name} {last_name}! Vous avez fait {int(delta_hours)} heures de {sport} le {formatted_date}"
            #print(f"Message envoyé: {text_msg}")
            slack_client.chat_postMessage(channel=SLACK_CHANNEL, text=text_msg, username=SLACK_USERNAME)
        except SlackApiError as e:
            print(f"Erreur Slack: {e.response['error']}")

slack_query = (
    activities_df.writeStream
    .foreachBatch(send_to_slack)
    .outputMode("append")
    .start()
)

# Keep both running
spark.streams.awaitAnyTermination()

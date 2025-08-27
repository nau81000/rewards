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
from sqlalchemy import create_engine, text
import pandas as pd
import googlemaps
import time

# Init flag
init = True

# Load environment variables
load_dotenv()
SLACK_CHANNEL = getenv("SLACK_CHANNEL")
SLACK_USERNAME = getenv("SLACK_USERNAME")
OFFICE_ADDRESS = getenv("OFFICE_ADDRESS")
MAX_HOME_DISTANCE_WALK=getenv("MAX_HOME_DISTANCE_WALK")
MAX_HOME_DISTANCE_OTHER=getenv("MAX_HOME_DISTANCE_WALK")
MIN_ACTIVITIES_YEAR=getenv("MAX_HOME_DISTANCE_WALK")
INCOME_PERCENT_REWARD=getenv("MAX_HOME_DISTANCE_WALK")
EXTRA_DAYS_REWARD=getenv("MAX_HOME_DISTANCE_WALK")

# Connect to DB
engine = create_engine(getenv("SPORT_DATA_SQL_ALCHEMY_CONN"))

# Open Slack channel
slack_client = WebClient(token=getenv("SLACK_TOKEN")) 

# Initialiser le client
gmaps = googlemaps.Client(key=getenv("GOOGLE_MAPS_API_KEY"))

# Create Spark session
spark = (
    SparkSession.builder
    .appName("RedpandaToDelta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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

# === Query A: Write to Delta Lake ===
# TODO: Calculate prime and extra days : enhance employee table
# Update employee data on change
#delta_query = (
#     raw_df.writeStream
#     .format("delta")
#     .outputMode("append")
#     .option("checkpointLocation", "/opt/spark-checkpoints/activities_checkpoint")
#     .start("/opt/spark-data/delta")
#)

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

def geocode_addresses(addresses):
    """ Get latitude, longitude from addresses
    """
    coords = []
    for address in addresses:
        try:
            result = gmaps.geocode(address)
            if result:
                location = result[0]['geometry']['location']
                coords.append((location['lat'], location['lng']))
            else:
                print(f"Adresse introuvable : {address}")
                coords.append(None)
        except Exception as e:
            print(f"Erreur de géocodage pour {address}: {e}")
            coords.append(None)
    return coords

def update_rewards():
    """ Update rewards parquet file
    """
    # Get all employees
    df_employees = pd.read_sql_query('SELECT id_employee, income, id_movement_means, address FROM employees', engine)
    # Build GPS coordinates
    coords_list = geocode_addresses(df_employees["address"].tolist())
    office_coords = geocode_addresses([OFFICE_ADDRESS])

    # Tool to split in 25 bunches (Google Maps API constraints)
    def chunk_list(lst, size=25):
        for i in range(0, len(lst), size):
            yield lst[i:i + size]

    distances = []
    # Calcul par batch de 25 destinations max
    for batch in chunk_list(coords_list):
        valid_coords = [c for c in batch if c is not None]
        if not valid_coords:
            distances.extend([None] * len(batch))
            continue
        try:
            result = gmaps.distance_matrix(
                origins=office_coords,
                destinations=valid_coords,
                mode="driving",
                language="fr",
                units="metric"
            )
            
            batch_distances = []
            idx = 0
            for c in batch:
                if c is None:
                    batch_distances.append(None)
                else:
                    element = result['rows'][0]['elements'][idx]
                    if element['status'] == 'OK':
                        batch_distances.append(element['distance']['value'] / 1000)  # km
                    else:
                        batch_distances.append(None)
                    idx += 1

            distances.extend(batch_distances)

        except Exception as e:
            print(f"Erreur API pour un batch: {e}")
            distances.extend([None] * len(batch))
        time.sleep(1)  # Avoid API saturation

    # Add distance column
    df_employees["distance_m"] = distances
    # 
    print(df_employees.head(2))

def process_message(batch_df, _):
    """ Process messages from Redpanda Stream
    """
    global init

    if init:
        # Create rewards parquet the first time
        update_rewards()
        init = False
    # Send Slack message
    for row in batch_df.collect():
        with engine.connect() as conn:
            # Difference in microseconds
            delta_us = row['end_date'] - row['start_date']
            # Convert in hours
            delta_hours = delta_us / 1_000_000 / 3600
            # Get the activity date in french
            start_date = datetime.fromtimestamp(row['start_date'] / 1_000_000, tz=ZoneInfo("Europe/Paris"))
            formatted_date = format_datetime(start_date, "EEEE d MMMM yyyy", locale="fr_FR")
            # Get employee first and last names
            employee = conn.execute(text("SELECT first_name, last_name FROM employees WHERE id_employee=:id_emp"), {'id_emp': row['id_employee']}).fetchone()
            # Get sport name
            sport = conn.execute(text("SELECT sport FROM sports WHERE id_sport=:id_sport"), {'id_sport': row['id_sport']}).fetchone()
            # Building message
            text_msg= f"Bravo {employee[0]} {employee[1]}! Vous avez fait {int(delta_hours)} heures de {sport[0]} le {formatted_date}"
            # Send Slack message
            try:
                slack_client.chat_postMessage(channel=SLACK_CHANNEL, text=text_msg, username=SLACK_USERNAME)
            except SlackApiError as e:
                print(f"Erreur Slack: {e.response['error']}")

slack_query = (
    activities_df.writeStream
    .foreachBatch(process_message)
    .outputMode("append")
    .start()
)

# Keep both running
spark.streams.awaitAnyTermination()

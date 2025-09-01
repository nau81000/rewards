from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType
from pyspark.sql.functions import col, from_json
from dotenv import load_dotenv
from os import getenv
from datetime import datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine, text
from zoneinfo import ZoneInfo
import pandas as pd
import time
import googlemaps
import threading

# Load environment variables
load_dotenv()
POSTGRES_DB_NAME = getenv('POSTGRES_DB_NAME')
POSTGRES_ADMIN_USER = getenv('POSTGRES_ADMIN_USER')
POSTGRES_ADMIN_PWD = getenv('POSTGRES_ADMIN_PWD')
POSTGRES_DB_HOST = getenv('POSTGRES_DB_HOST')
AWS_S3_ROOT_STORAGE = getenv("AWS_S3_ROOT_STORAGE")
OFFICE_ADDRESS = getenv("OFFICE_ADDRESS")
MAX_HOME_DISTANCE_WALK=getenv("MAX_HOME_DISTANCE_WALK")
MAX_HOME_DISTANCE_OTHER=getenv("MAX_HOME_DISTANCE_OTHER")
MIN_ACTIVITIES_YEAR=getenv("MIN_ACTIVITIES_YEAR")
INCOME_PERCENT_REWARD=getenv("INCOME_PERCENT_REWARD")
EXTRA_DAYS_REWARD=getenv("EXTRA_DAYS_REWARD")

# Declare functions

def geocode_addresses(gmaps, addresses):
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

def count_activities(engine, id_employee):
    """ Count the number of activities for an employee in the past year
    """
    # What's happened the past year 
    one_year_ago = datetime.now(ZoneInfo("Europe/Paris")) - relativedelta(years=1)
    count = 0
    with engine.connect() as conn:
        count = conn.execute(
            text("SELECT count(*) FROM activities WHERE id_employee=:id_emp AND start_date>=:date_ref")
            , {'id_emp': id_employee, 'date_ref': one_year_ago.strftime('%Y-%m-%d %H:%M:%S')}).fetchone()[0]
    return count

def update_rewards():
    """ Update rewards and save them in Delta Lake
    """
    global spark

    # Connect to DB
    engine = create_engine(getenv("SPORT_DATA_SQL_ALCHEMY_CONN"))

    # Initialiser le client
    gmaps = googlemaps.Client(key=getenv("GOOGLE_MAPS_API_KEY"))

    # Get all employees
    with engine.connect() as conn:
        df_employees = pd.read_sql_query('SELECT id_employee, income, id_movement_means, address FROM employees', con=conn)
    # Build GPS coordinates
    coords_list = geocode_addresses(gmaps, df_employees["address"].tolist())
    office_coords = geocode_addresses(gmaps, [OFFICE_ADDRESS])

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
    df_employees["home_office_distance_km"] = distances
    # Add bonus column
    df_employees["bonus"] = df_employees.apply(
        lambda row: row["income"]*(int(INCOME_PERCENT_REWARD)*1.0/100) if row["id_movement_means"] > 3 else 0, axis=1
    )
    # Add movement_means_error column
    df_employees["movement_means_anomaly"] = df_employees.apply(
        lambda row: True if (row["id_movement_means"] == 3 
                             and row["home_office_distance_km"] >= int(MAX_HOME_DISTANCE_WALK)) 
                             or (row["id_movement_means"] == 4 
                                 and row["home_office_distance_km"] >= int(MAX_HOME_DISTANCE_OTHER)) else False
        , axis=1
        )
    # Add extra_days column
    df_employees["extra_days"] = df_employees.apply(
        lambda row: int(EXTRA_DAYS_REWARD) if count_activities(engine, row["id_employee"]) >= int(MIN_ACTIVITIES_YEAR) else 0
        , axis=1)

    df_employees.drop(columns=['income', 'address', 'id_movement_means'], inplace=True)

    # Write in Delta Lake    
    (spark.createDataFrame(df_employees)).write.format("delta").mode("overwrite").save(AWS_S3_ROOT_STORAGE + "/delta/rewards")

def idle_action():
    """ Update rewards once there no more activities streamed
    """
    print("***** Updating rewards!", flush=True)
    update_rewards()

def schedule_idle_timer():
    """ Schedule a timer to call the idel action
    """
    global idle_timer, timer_lock

    with timer_lock:
        if idle_timer:
            idle_timer.cancel()  # cancel previous timer
        idle_timer = threading.Timer(DEBOUNCE_SECONDS, idle_action)
        idle_timer.start()

def process_batch(batch_df, _):
    """ Append activities in S3 bucket (/delta/activities)
        call timer if idle
    """
    if batch_df.count() > 0:
        # Cancel previous timer and schedule a new one
        schedule_idle_timer()
        # Write to S3
        (batch_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(AWS_S3_ROOT_STORAGE + "/delta/activities"))

#### Main part

# Global state
idle_timer = None
timer_lock = threading.Lock()
DEBOUNCE_SECONDS = 30

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
    .option("failOnDataLoss", "false")
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

# Step 5: Get sports table
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

query = (
    joined_df.writeStream
    .foreachBatch(process_batch)
    .outputMode("append")
    .option("checkpointLocation", AWS_S3_ROOT_STORAGE + "/checkpoints/activities")
    .start()
)

query.awaitTermination()

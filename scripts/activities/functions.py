"""
Helper functions to manage activities 
"""
from airflow.decorators import task
from dotenv import load_dotenv
from os import getenv
import pandas as pd
import psycopg2 as pg

@task(task_id="get_activities_from_strava")
def get_activities_from_strava ():
    """ Get activities from Strava
    """

@task(task_id="import_hr")
def import_hr ():
    """ Import human resources data from url
    """
    # Load environment
    load_dotenv()
    # Read HR data
    df_hr = pd.read_excel(getenv("RH_DATA_FILE"))
    print(df_hr.head())
    # Read sports data
    df_sports = pd.read_excel(getenv("SPORTS_DATA_FILE"))
    print(df_sports.head())
    # Connection to DB
    print(getenv("POSTGRES_DB"), getenv("POSTGRES_ADMIN_USER"), getenv("POSTGRES_ADMIN_PWD"))
    conn = pg.connect(
        dbname = getenv("POSTGRES_DB"), 
        host = "postgres",
        user = getenv("POSTGRES_ADMIN_USER"), 
        password = getenv("POSTGRES_ADMIN_PWD"),
        port = 5432
    )
    conn.close()

@task(task_id="generate_activities")
def generate_activities_from_scratch ():
    """ Generate activities from scratch into a SQlite file
    """
    print('generate_activities_from_scratch')

@task(task_id="check_activities")
def check_activities ():
    """ Check activities from scratch into a SQlite file
    """
    print('check_activities')

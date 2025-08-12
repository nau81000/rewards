"""
Helper functions to manage activities 
"""
from airflow.decorators import task
from os import getenv
from sqlalchemy import create_engine, exc
import pandas as pd
import psycopg2 as pg
import great_expectations as gx
import logging

def check_activity (df_employee):
    """ Check activity from data frame
    """
    # Be sure that date columns have right type
    df_employee['start_date'] = pd.to_datetime(df_employee['start_date'])
    df_employee['end_date'] = pd.to_datetime(df_employee['end_date'])
    # Create context
    context = gx.get_context()
    # Add a Pandas Data Source
    data_source = context.data_sources.add_pandas(name="activity")
    # Add a Data Asset to the Data Source
    data_asset = data_source.add_dataframe_asset(name="activity_asset")
    # Add the Batch Definition
    batch_definition = data_asset.add_batch_definition_whole_dataframe("activity_batch")
    # Create some expectations
    expectation_suite = gx.ExpectationSuite(name="expectation_suite")
    expectation_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
            column = "id_sport",
            value_set = list(range(1, 16))
        )
    )
    expectation_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(
            column = "start_date",
            type_ = "datetime64"
        )
    )
    expectation_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(
            column = "end_date",
            type_ = "datetime64"
        )
    )
    # Get the dataframe as a Batch
    batch_parameters = {"dataframe": df_employee}
    batch = batch_definition.get_batch(batch_parameters=batch_parameters)
    # Test the Expectation
    return batch.validate(expectation_suite)

@task(task_id="get_activities")
def get_activities ():
    """ Get activities from a server
    """
    # Set great_expectations logging less verbose
    logging.getLogger("great_expectations").setLevel(logging.ERROR)
    # Connection to DB
    config = {
           'host': getenv("POSTGRES_DB_HOST"),
           'dbname': getenv("POSTGRES_DB_NAME"), 
           'user': getenv("POSTGRES_ADMIN_USER"), 
           'password': getenv("POSTGRES_ADMIN_PWD"),
           'port': 5432
    }
    db = create_engine(getenv("SPORT_DATA_SQL_ALCHEMY_CONN"))
    conn = db.connect()
    conn1 = pg.connect(**config)
    conn1.autocommit = True
    cursor = conn1.cursor()
    # Get all employee ids first
    cursor.execute("SELECT id_employee FROM employees ORDER BY id_employee")
    employees = cursor.fetchall()
    # Get activities for each employee
    for row in employees:
        employee = row[0]
        df_employee = pd.read_json(f"http://strava-like-server:5000/employee/{employee}")
        res = check_activity(df_employee)
        print(res)
        if res['success']:
            try:
                df_employee.to_sql('activities', conn, if_exists= 'append', index=False)
            except Exception as e:
                # TODO
                pass
    conn1.close()
    conn.close()

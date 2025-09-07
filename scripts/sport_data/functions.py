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

def declared_activity_yet (cursor, df_employee):
    """ Check whether an activity is already declared or not
    """
    values = df_employee.iloc[0]
    cursor.execute(
        f"SELECT COUNT(id_employee) FROM activities \
            WHERE id_employee={values['id_employee']} \
                  AND start_date='{values['start_date']}'"
    )
    return cursor.fetchone()[0]

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
    db = create_engine(getenv("POSTGRES_SQL_ALCHEMY_CONN"))
    conn = db.connect()
    conn1 = pg.connect(**config)
    conn1.autocommit = True
    cursor = conn1.cursor()
    # Get all employee ids first
    cursor.execute("SELECT id_employee FROM employees ORDER BY id_employee")
    employees = cursor.fetchall()
    # Create context to check activities
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
    # Get activities for each employee
    for row in employees:
        # Get activity from server
        df_employee = pd.read_json(f"http://strava-like-server:5000/employee/{row[0]}")
        # Be sure that some columns have right type
        df_employee['start_date'] = pd.to_datetime(df_employee['start_date'])
        df_employee['end_date'] = pd.to_datetime(df_employee['end_date'])
        # Check data
        batch = batch_definition.get_batch(batch_parameters={"dataframe": df_employee})
        validity = batch.validate(expectation_suite)
        if validity['success']:
            try:
                if not declared_activity_yet(cursor, df_employee):
                    df_employee.to_sql('activities', conn, if_exists= 'append', index=False)
            except Exception as e:
                # TODO
                pass
    conn1.close()
    conn.close()

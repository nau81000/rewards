"""
Helper functions to manage activities 
"""
from sqlalchemy import create_engine, exc
from dotenv import load_dotenv
from os import getenv
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
import sys
import pandas as pd
import psycopg2 as pg
import numpy as np

def create_tables(cursor, conn1):
    """
    """
    tables= {
         'movement_means': {
            'id': 'int PRIMARY KEY',
            'name': 'text'
        },
        'contract_types': {
            'id': 'int PRIMARY KEY',
            'name': 'text'
        },
        'business_units': {
            'id': 'int PRIMARY KEY',
            'name': 'text'
        },
        'sports': {
            'id': 'int PRIMARY KEY',
            'name': 'text'
        },
        'employees': {
            'id_employee': 'int PRIMARY KEY',
            'last_name': 'text',
            'first_name': 'text',
            'birth_date': 'date',
            'hire_date': 'date',
            'income': 'int',
            'vacation_days': 'int',
            'address': 'text',
            'id_bu': 'int REFERENCES business_units (id)',
            'id_movement_means': 'int',
            'id_contract_type': 'int REFERENCES contract_types (id)',
            'id_sport': 'int'
        },
        'activities': {
            'id': 'SERIAL PRIMARY KEY',
            'id_employee': 'int REFERENCES employees (id_employee)',
            'id_sport': 'int REFERENCES sports (id)',
            'distance': 'float',
            'start_date': 'timestamp NOT NULL',
            'end_date': 'timestamp NOT NULL',
            'comment': 'text'
        }
    }
    for table in tables:
        # drop table if it already exists
        #cursor.execute(f"DROP TABLE if exists {table}")
        # Create table
        sql = f"CREATE TABLE {table}"
        first = True
        for key, value in tables[table].items():
            if first:
                sql += '('
                first = False
            else:
                sql += ','
            sql += f"{key} {value}"
        sql += ");"
        try:
            cursor.execute(sql)
        except pg.errors.DuplicateTable:
            # The table already exists
            pass
        conn1.commit()

def import_records(conn, dataframe, table_name):
    """ Import records in table
    """
    try:
        dataframe.to_sql(table_name, conn, if_exists= 'append', index=False)
    except exc.IntegrityError:
        # The primary key already exists
        pass

def import_tables(conn):
    """ Import human resources data from url
    """
    # Read HR data
    df_hr = pd.read_excel(getenv("SRC_RH_DATA_FILE"))
    # Add an employee leading to an error in movement_means consistency
    df_hr.loc[len(df_hr)] = [
        100000, "Doe", "John", "1/1/1970", "bu", "1/1/2025",
        30000, "CDI", 25, "1 Place du Vigan, 81000 Albi", "Marche/running"
    ]
    # Rename columns
    df_hr.rename(inplace=True,
        columns={
            "ID salarié": "id_employee",
            "Nom": "last_name",
            "Prénom": "first_name",
            "Date de naissance": "birth_date",
            "BU": "bu",
            "Date d'embauche": "hire_date",
            "Salaire brut": "income",
            "Type de contrat": "contract_type",
            "Nombre de jours de CP": "vacation_days",
            "Adresse du domicile": "address",
            "Moyen de déplacement": "movement_means"
        }
    )
    # Create business unit dataframe
    df_bu = pd.DataFrame(df_hr['bu'].drop_duplicates().reset_index(drop=True))
    df_bu['id_bu'] = df_bu.index + 1  # IDs à partir de 1    
    # Merge with employee dataframe
    df_hr = df_hr.merge(df_bu, on='bu', how='left').drop(columns=['bu'])
    # Rename columns
    df_bu.rename(columns={'id_bu': 'id', 'bu': 'name'}, inplace=True)
    # Create movement_means dataframe
    df_mm = pd.DataFrame(df_hr['movement_means'].drop_duplicates().reset_index(drop=True))
    df_mm['id_movement_means'] = df_mm.index + 1  # IDs à partir de 1    
    # Merge with employee dataframe
    df_hr = df_hr.merge(df_mm, on='movement_means', how='left').drop(columns=['movement_means'])
    # Rename columns
    df_mm.rename(columns={'id_movement_means': 'id', 'movement_means': 'name'}, inplace=True)
    # Create contract type dataframe
    df_contract_types = pd.DataFrame(df_hr['contract_type'].drop_duplicates().reset_index(drop=True))
    df_contract_types['id_contract_type'] = df_contract_types.index + 1  # IDs à partir de 1    
    # Merge with employee dataframe
    df_hr = df_hr.merge(df_contract_types, on='contract_type', how='left').drop(columns=['contract_type'])
    # Rename columns
    df_contract_types.rename(columns={'id_contract_type': 'id', 'contract_type': 'name'}, inplace=True)
    # Read sports data
    df_employee_sports = pd.read_excel(getenv("SRC_SPORTS_DATA_FILE"))
    # Add data for John Doe
    df_employee_sports.loc[len(df_employee_sports)] = [
        100000, pd.NA
    ]
    df_employee_sports.rename(inplace=True,
        columns={
            "ID salarié": "id_employee",
            "Pratique d'un sport": "sport",
        }
    )    
    # Create sports dataframe
    df_sports = pd.DataFrame(df_employee_sports['sport'].drop_duplicates().dropna().reset_index(drop=True))
    df_sports['id_sport'] = df_sports.index + 1  # IDs à partir de 1
    # Merge with employee sports dataframe
    df_employee_sports = df_employee_sports.merge(df_sports, on='sport', how='left').drop(columns=['sport'])
    df_employee_sports['id_sport'] = df_employee_sports['id_sport'].fillna(0).astype(int)
    # Finally merge with employee dataframe
    df_hr = df_hr.merge(df_employee_sports, on='id_employee', how='left')
    # Rename columns
    df_sports.rename(columns={'id_sport': 'id', 'sport': 'name'}, inplace=True)
    # Push records into tables
    import_records(conn, df_bu, 'business_units', )
    import_records(conn, df_mm, 'movement_means')
    import_records(conn, df_contract_types, 'contract_types')
    import_records(conn, df_sports, 'sports')
    import_records(conn, df_hr, 'employees')
    return df_hr

def generate_activities_days(date_ref):
    """ Generate two days activities per month on sunday
        around the 5th and 20th
    """
    def nearest_sunday(year, month, day):
        """ Find the nearest sunday around a given date
        """
        target = date(year, month, day)
        # Sunday is day of week 6
        offset = (6 - target.weekday()) % 7
        before_offset = (target.weekday() - 6) % 7
        # Choose the nearest
        if before_offset <= offset:
            return target - timedelta(days=before_offset)
        else:
            return target + timedelta(days=offset)

    # Choose two dates per month in the last past year
    dates = []
    for i in range(12):
        month_date = date_ref - relativedelta(months=i)
        year, month = month_date.year, month_date.month
        sunday_near_5 = nearest_sunday(year, month, 5)
        sunday_near_20 = nearest_sunday(year, month, 20)
        dates.extend([sunday_near_5, sunday_near_20])
    # Sort the dates
    dates = sorted(set(dates))
    # Keep only dates <= date_ref
    return [d for d in dates if d <= date_ref]

def generate_activities(conn, df_hr):
    """ Simulate activities for employees

    For each employees declaring an usual sport, we'll create 24 activities (2 per month)
    for the past year
    """
    # Generate dates from today
    dates = generate_activities_days(date.today())
    # Create activities dataframe
    df_activities = df_hr.loc[df_hr['id_sport'] > 0,['id_employee', 'id_sport']]
    df_activities['comment'] = 'super'
    # Set 10000 meters distance for hiking, triathlon and runing, 0 for any other activity
    df_activities['distance'] = np.where(df_activities["id_sport"].isin([4, 5, 6]), 10000, 0)
    df_activities['start_date'] = str(dates[0]) + ' 08:00:00'
    df_activities['end_date'] = str(dates[0]) + ' 10:00:00'
    # Create duplicates
    df_dupes = pd.concat(
        [df_activities.assign(start_date=(str(dates[idx]) + ' 08:00:00'), end_date=(str(dates[idx]) + ' 10:00:00'))
        for idx in range(1, len(dates))],
        ignore_index=True
    )
    # Merge original + duplicates
    df_result = pd.concat([df_activities, df_dupes], ignore_index=True)
    import_records(conn, df_result, 'activities')

def main():
    """ Create tables
        Initialize tables
    """
    # Load environment variables first
    load_dotenv()
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
    # Create tables
    create_tables(cursor, conn1)
    # Import tables
    df_hr = import_tables(conn)
    # Generate activities
    generate_activities(conn, df_hr)
    # Close connections
    conn1.close()
    conn.close()
    return 0

if __name__ == '__main__':
    sys.exit(main())  # next section explains the use of sys.exit

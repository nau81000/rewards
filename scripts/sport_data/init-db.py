"""
Helper functions to manage activities 
"""
from sqlalchemy import create_engine, exc
from dotenv import load_dotenv
from os import getenv
import sys
import pandas as pd
import psycopg2 as pg

def create_tables(cursor, conn1):
    """
    """
    tables= {
        'movement_means': {
            'id_movement_means': 'int PRIMARY KEY',
            'movement_means': 'text'
        },
        'contract_types': {
            'id_contract_type': 'int PRIMARY KEY',
            'contract_type': 'text'
        },
        'business_units': {
            'id_bu': 'int PRIMARY KEY',
            'bu': 'text'
        },
        'sports': {
            'id_sport': 'int PRIMARY KEY',
            'sport': 'text'
        },
        'activities': {
            'id_employee': 'int PRIMARY KEY',
            'name': 'text',
            'type': 'text',
            'sport_type': 'text',
            'description': 'text',
            'start_date_local': 'date',
            'elapsed_time': 'int',
            'distance': 'float',
            'trainer': 'int',
            'commute': 'int'
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
            'id_bu': 'int REFERENCES business_units (id_bu)',
            'id_movement_means': 'int',
            'id_contract_type': 'int REFERENCES contract_types (id_contract_type)',
            'id_sport': 'int'
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

def import_records(dataframe, table_name, conn):
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
    df_hr = pd.read_excel(getenv("RH_DATA_FILE"))
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
    # And merge with employee dataframe
    df_hr = df_hr.merge(df_bu, on='bu', how='left').drop(columns=['bu'])
    # Create movement_means dataframe
    df_mm = pd.DataFrame(df_hr['movement_means'].drop_duplicates().reset_index(drop=True))
    df_mm['id_movement_means'] = df_mm.index + 1  # IDs à partir de 1    
    # And merge with employee dataframe
    df_hr = df_hr.merge(df_mm, on='movement_means', how='left').drop(columns=['movement_means'])
    # Create contract type dataframe
    df_contract_types = pd.DataFrame(df_hr['contract_type'].drop_duplicates().reset_index(drop=True))
    df_contract_types['id_contract_type'] = df_contract_types.index + 1  # IDs à partir de 1    
    # And merge with employee dataframe
    df_hr = df_hr.merge(df_contract_types, on='contract_type', how='left').drop(columns=['contract_type'])
    # Read sports data
    df_employee_sports = pd.read_excel(getenv("SPORTS_DATA_FILE"))
    df_employee_sports.rename(inplace=True,
        columns={
            "ID salarié": "id_employee",
            "Pratique d'un sport": "sport",
        }
    )    
    # Create sports dataframe
    df_sports = pd.DataFrame(df_employee_sports['sport'].drop_duplicates().dropna().reset_index(drop=True))
    df_sports['id_sport'] = df_sports.index + 1  # IDs à partir de 1
    # And merge with employee sports dataframe
    df_employee_sports = df_employee_sports.merge(df_sports, on='sport', how='left').drop(columns=['sport'])
    df_employee_sports['id_sport'] = df_employee_sports['id_sport'].fillna(0).astype(int)
    # Finally merge with employee dataframe
    df_hr = df_hr.merge(df_employee_sports, on='id_employee', how='left')
    # Push records into tables
    import_records(df_bu, 'business_units', conn)
    import_records(df_mm, 'movement_means', conn)
    import_records(df_contract_types, 'contract_types', conn)
    import_records(df_sports, 'sports', conn)
    import_records(df_hr, 'employees', conn)

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
    db = create_engine(getenv("SPORT_DATA_SQL_ALCHEMY_CONN"))
    conn = db.connect()
    conn1 = pg.connect(**config)
    conn1.autocommit = True
    cursor = conn1.cursor()
    # Create tables
    create_tables(cursor, conn1)
    # Import tables
    import_tables(conn)
    conn1.close()
    conn.close()
    return 0

if __name__ == '__main__':
    sys.exit(main())  # next section explains the use of sys.exit

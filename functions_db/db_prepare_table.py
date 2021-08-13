import psycopg2
import toml
data_toml = toml.load("config.toml")

site = data_toml['site_url']
host = data_toml['host']
database = data_toml['database']
user = data_toml['user']
password = data_toml['password']
port= data_toml['port']


table_all = data_toml['table_all']
table_general = data_toml['table_general']
table_details = data_toml['table_details']

def db_prepare_table():

    #table_all
    # Connect to the PostgreSQL database server
    postgresConnection = psycopg2.connect(host=host, port=port, dbname=database, user=user, password=password)
    # Get cursor object from the database connection
    cursor= postgresConnection.cursor()
    #for table in name_Table:
    # Create table statement
    sqlCreateTable = "CREATE TABLE IF NOT EXISTS "+table_all+"(site varchar(600),keyword varchar(600),gt_parsed integer,ga_parsed integer,gt_accuracy integer,gsc_date varchar(50),gsc_device varchar(60),gsc_ctr decimal,gsc_avg_pos decimal,gsc_sum_imp integer,gsc_sum_clic integer,gsc_page varchar(1000),ga_search_volume integer,ga_competition decimal,ga_average_cpc integer,gt_dates Json,forecast_dates Json,last_trend integer,last_forecast integer);"
    #sqlCreateTable = "CREATE TABLE IF NOT EXISTS "+table+";"
    # Create a table in PostgreSQL database
    cursor.execute(sqlCreateTable)
    postgresConnection.commit()

    #table_general
    # Connect to the PostgreSQL database server
    postgresConnection = psycopg2.connect(host=host, port=port, dbname=database, user=user, password=password)
    # Get cursor object from the database connection
    cursor= postgresConnection.cursor()
    #for table in name_Table:
    # Create table statement
    sqlCreateTable = "CREATE TABLE IF NOT EXISTS "+table_general+"(site text,gt_dates Json,forecast_dates Json);"
    #sqlCreateTable = "CREATE TABLE IF NOT EXISTS "+table+";"
    # Create a table in PostgreSQL database
    cursor.execute(sqlCreateTable)
    postgresConnection.commit()

    #table_details
    # Connect to the PostgreSQL database server
    postgresConnection = psycopg2.connect(host=host, port=port, dbname=database, user=user, password=password)
    # Get cursor object from the database connection
    cursor= postgresConnection.cursor()
    #for table in name_Table:
    # Create table statement
    sqlCreateTable = "CREATE TABLE IF NOT EXISTS "+table_details+"(type_gt_or_forecast text,site varchar(600),keyword varchar(600),gt_parsed integer,ga_parsed integer,gt_accuracy integer,gsc_date varchar(50),gsc_device varchar(60),gsc_ctr decimal,gsc_avg_pos decimal,gsc_sum_imp integer,gsc_sum_clic integer,gsc_page varchar(1000),ga_search_volume integer,ga_competition decimal,ga_average_cpc integer,last_trend integer,last_forecast integer);"
    #sqlCreateTable = "CREATE TABLE IF NOT EXISTS "+table+";"
    # Create a table in PostgreSQL database
    cursor.execute(sqlCreateTable)
    postgresConnection.commit()




db_prepare_table()
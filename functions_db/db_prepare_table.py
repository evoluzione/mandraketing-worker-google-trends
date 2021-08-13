import psycopg2
import toml
data_toml = toml.load("config.toml")

site = data_toml['site_url']
host = data_toml['host_test']
database = data_toml['database_test']
user = data_toml['user_test']
password = data_toml['password_test']
port= data_toml['port_test']


table_all = data_toml['table_all']
table_details = data_toml['table_details']
table_general = data_toml['table_general']

def db_prepare_table():

    # Connect to the PostgreSQL database server
    postgresConnection = psycopg2.connect(host=host, port=port, dbname=database, user=user, password=password)

    # Get cursor object from the database connection
    cursor= postgresConnection.cursor()
    name_Table= [table_all,table_details,table_general]
    for table in name_Table:
        # Create table statement
        sqlCreateTable = "create table "+table+" (id bigint, title varchar(128), summary varchar(256), story text);"
        # Create a table in PostgreSQL database
        cursor.execute(sqlCreateTable)
        postgresConnection.commit()
        # Get the updated list of tables
        sqlGetTableList = "SELECT table_schema,table_name FROM information_schema.tables where table_schema='test' ORDER BY table_schema,table_name ;"
        #sqlGetTableList = "\dt"
        # Retrieve all the rows from the cursor
        cursor.execute(sqlGetTableList)
        tables = cursor.fetchall()
        # Print the names of the tables
        for table in tables:
            print(table)

db_prepare_table()
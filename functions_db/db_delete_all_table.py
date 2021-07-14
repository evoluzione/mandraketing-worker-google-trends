import psycopg2
import toml

data_toml = toml.load("config.toml")

site = data_toml['site_url']
host = data_toml['host']
database = data_toml['database']
user = data_toml['user']
password = data_toml['password']
port= data_toml['port']

host_general = data_toml['host_general']
database_general = data_toml['database_general']
user_general = data_toml['user_general']
password_general = data_toml['password_general']
port_general = data_toml['port_general']

host_details = data_toml['host_details']
database_details = data_toml['database_details']
user_details = data_toml['user_details']
password_details = data_toml['password_details']
port_details = data_toml['port_details']

def db_delete_all_table():
   conn = psycopg2.connect(
      database=database, user=user, password=password, host=host, port= port
   )
   conn.autocommit = True
   cursor = conn.cursor()
   cursor.execute('''DROP TABLE wtforecast;''')
   conn.commit()
   conn.close()

def db_delete_all_table_general():
   conn = psycopg2.connect(
      database=database_general, user=user_general, password=password_general, host=host_general, port= port_general
   )
   conn.autocommit = True
   cursor = conn.cursor()
   cursor.execute('''DROP TABLE wtforecastgeneral;''')
   conn.commit()
   conn.close()

def db_delete_all_table_details():
   conn = psycopg2.connect(
      database=database_details, user=user_details, password=password_details, host=host_details, port= port_details
   )
   conn.autocommit = True
   cursor = conn.cursor()
   cursor.execute('''DROP TABLE wtforecastdetails;''')
   conn.commit()
   conn.close()

db_delete_all_table()
db_delete_all_table_general()
db_delete_all_table_details()
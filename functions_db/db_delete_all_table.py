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


def db_delete_all_table():
   conn = psycopg2.connect(
      database=database, user=user, password=password, host=host, port= port
   )
   conn.autocommit = True
   cursor = conn.cursor()
   cursor.execute(f'''DROP TABLE {table_all};''')
   conn.commit()
   conn.close()

   conn = psycopg2.connect(
      database=database, user=user, password=password, host=host, port= port
   )
   conn.autocommit = True
   cursor = conn.cursor()
   cursor.execute(f'''DROP TABLE {table_general};''')
   conn.commit()
   conn.close()

   conn = psycopg2.connect(
      database=database, user=user, password=password, host=host, port= port
   )
   conn.autocommit = True
   cursor = conn.cursor()
   cursor.execute(f'''DROP TABLE {table_details};''')
   conn.commit()
   conn.close()



db_delete_all_table()
import psycopg2
import toml
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
data_toml = toml.load("config.toml")

site = data_toml['site_url']
host = data_toml['host_general']
database = data_toml['database_general']
user = data_toml['user_general']
password = data_toml['password_general']
port= data_toml['port_general']

def db_prepare_general():
   #establishing the connection
   #check if table exist
   conn = psycopg2.connect(
      database=database, user=user, password=password, host=host, port= port
   )
   cur = conn.cursor()
   cur.execute("select * from information_schema.tables where table_name=%s", ('wtforecastgeneral',))
   result = bool(cur.rowcount)
   print(result)
   conn.close()

   if result != True:
      conn = psycopg2.connect(
         database=database, user=user, password=password, host=host, port= port
      )
      conn.autocommit = True
      cursor = conn.cursor()
      cursor.execute('''CREATE TABLE wtforecastgeneral (
                     site text,
                     gt_dates Json,
                     forecast_dates Json
                     );''')
      conn.commit()
      conn.close()
      print('database creato')

   else:
      print('DB già presente')
      conn = psycopg2.connect(
         database=database, user=user, password=password, host=host, port= port
      )
      conn.autocommit = True
      cursor = conn.cursor()
      cursor.execute(f'''DELETE FROM wtforecastgeneral WHERE "site" = '{site}';''')
      print('eliminate righe con dominio')
      conn.commit()
      conn.close()


db_prepare_general()
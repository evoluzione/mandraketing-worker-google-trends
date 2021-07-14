import psycopg2
import toml
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
data_toml = toml.load("config.toml")

site = data_toml['site_url']
host = data_toml['host_details']
database = data_toml['database_details']
user = data_toml['user_details']
password = data_toml['password_details']
port= data_toml['port_details']

def db_prepare_details():
   #establishing the connection
   #check if table exist
   conn = psycopg2.connect(
      database=database, user=user, password=password, host=host, port= port
   )
   cur = conn.cursor()
   cur.execute("select * from information_schema.tables where table_name=%s", ('wtforecastdetails',))
   result = bool(cur.rowcount)
   print(result)
   conn.close()

   if result != True:
      conn = psycopg2.connect(
         database=database, user=user, password=password, host=host, port= port
      )
      conn.autocommit = True
      cursor = conn.cursor()
      cursor.execute('''CREATE TABLE wtforecastdetails (
                     type_gt_or_forecast text,
                     site varchar(600),
                     keyword varchar(600),
                     gt_parsed integer,
                     ga_parsed integer,
                     gt_accuracy integer,
                     gsc_date varchar(50),
                     gsc_device varchar(60),
                     gsc_ctr decimal,
                     gsc_avg_pos decimal,
                     gsc_sum_imp integer,
                     gsc_sum_clic integer,
                     gsc_page varchar(1000),
                     ga_search_volume integer,
                     ga_competition decimal,
                     ga_average_cpc integer,
                     last_trend integer,
                     last_forecast integer
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
      cursor.execute(f'''DELETE FROM wtforecastdetails WHERE "site" = '{site}';''')
      print('eliminate righe con dominio')
      conn.commit()
      conn.close()


db_prepare_details()
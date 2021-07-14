import psycopg2
import toml
data_toml = toml.load("config.toml")

site = data_toml['site_url']
host = data_toml['host']
database = data_toml['database']
user = data_toml['user']
password = data_toml['password']
port= data_toml['port']

def db_prepare_cruncher():
   #establishing the connection
   #check if table exist
   conn = psycopg2.connect(
      database=database, user=user, password=password, host=host, port= port
   )
   cur = conn.cursor()
   cur.execute("select * from information_schema.tables where table_name=%s", ('wtforecast',))
   result = bool(cur.rowcount)
   print(result)
   conn.close()

   if result != True:
      conn = psycopg2.connect(
         database=database, user=user, password=password, host=host, port= port
      )
      conn.autocommit = True
      cursor = conn.cursor()
      cursor.execute('''CREATE TABLE wtforecast (
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
                     gt_dates Json,
                     forecast_dates Json,
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
      cursor.execute(f'''DELETE FROM wtforecast WHERE "site" = '{site}';''')
      print('eliminate righe con dominio')
      conn.commit()
      conn.close()


#db_prepare_cruncher()
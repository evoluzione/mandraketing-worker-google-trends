import pandas as pd
import psycopg2 as pg
import psycopg2
from sqlalchemy import create_engine
from functions_db.db_prepare_details import db_prepare_details

db_prepare_details()
import toml
data_toml = toml.load("config.toml")

site = data_toml['site_url']
host = data_toml['host_details']
database = data_toml['database_details']
user = data_toml['user_details']
password = data_toml['password_details']
port= data_toml['port_details']

conn = psycopg2.connect(
    database=database, user=user, password=password, host=host, port= port
)
conn.autocommit = True
cursor = conn.cursor()
cursor.execute(f'''DELETE FROM wtforecast WHERE "site" = '{site}';''')
print('eliminate righe con dominio')
conn.commit()
conn.close()

site_url = data_toml['site_url']
n_keywords = data_toml['n_keywords']
range_forecast = data_toml['range_forecast']
greater_than_trend = data_toml['greater_than_trend']
greater_than_feracast = data_toml['greater_than_feracast']

host = data_toml['host']
database = data_toml['database']
user = data_toml['user']
password = data_toml['password']
port = data_toml['port']
postgre_complete_url = data_toml['postgre_complete_url']

host_general = data_toml['host_details']
database_general = data_toml['database_details']
user_general = data_toml['user_details']
password_general = data_toml['password_details']
port_general = data_toml['port_details']
postgre_complete_url_general = data_toml['postgre_complete_url_details']

# create row in database
print(site_url)
print(type(site_url))

#
#
# Tabelle generali

# creazione df trend
engine_table = create_engine(postgre_complete_url)
df_trend_table = pd.read_sql_query(f"SELECT * FROM wtforecast WHERE site = '{site_url}';",con=engine_table)
df_last_trend = df_trend_table.copy()
df_last_trend = df_last_trend.loc[df_last_trend['last_trend'] > greater_than_trend]
df_last_trend['type_gt_or_forecast'] = 'type_gt'
#df_last_trend = df_last_trend[['keyword','ga_search_volume','ga_competition','gsc_avg_pos','gsc_sum_imp','gsc_sum_clic','last_trend']].copy()
print(df_last_trend)
del df_last_trend['gt_dates']
del df_last_trend['forecast_dates']
engine = create_engine(postgre_complete_url_general)
df_last_trend.to_sql('wtforecastdetails', engine, index=False, if_exists="append")


# creazione df forecasr
engine_table = create_engine(postgre_complete_url)
df_forecast_table = pd.read_sql_query(f"SELECT * FROM wtforecast WHERE site = '{site_url}';",con=engine_table)
df_last_forecast = df_forecast_table.copy()
df_last_forecast = df_last_forecast.loc[df_last_forecast['last_forecast'] > greater_than_feracast]
df_last_forecast['type_gt_or_forecast'] = 'type_forecast'
#df_last_trend = df_last_trend[['keyword','ga_search_volume','ga_competition','gsc_avg_pos','gsc_sum_imp','gsc_sum_clic','last_trend']].copy()
print(df_last_forecast)
del df_last_forecast['gt_dates']
del df_last_forecast['forecast_dates']
engine = create_engine(postgre_complete_url_general)
df_last_forecast.to_sql('wtforecastdetails', engine, index=False, if_exists="append")


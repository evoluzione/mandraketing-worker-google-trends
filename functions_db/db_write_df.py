from sqlalchemy import create_engine
import pandas as pd
import psycopg2 as pg
import psycopg2
import toml

data_toml = toml.load("config.toml")

host = data_toml['host']
database = data_toml['database']
user = data_toml['user']
password = data_toml['password']
port= data_toml['port']
postgre_complete_url = data_toml['postgre_complete_url']


engine = create_engine(postgre_complete_url)
df = pd.read_csv('02_google_search_console.csv', sep=';') 
df = df[["query", "date", "device", "ctr", "avg_position", "sumimppression", "sumclicks", "page"]]
df = df.rename(columns={'query': 'keywords', 'date': 'gsc_date'})
df = df.rename(columns={'device': 'gs_device', 'ctr': 'gs_ctr'})
df = df.rename(columns={ 'avg_position': 'gs_avg_pos', 'sumimppression': 'gs_sum_imp'})
df = df.rename(columns={'sumclicks': 'gs_sum_clic', 'page': 'gs_page'})

    
df.to_sql('wtforecast', engine, index=False)



# engine = pg.connect("dbname='defaultdb' user='avnadmin' host='wtforecast-ibeppo993-8940.aivencloud.com' port='17998' password='fjijq4gpz62udo3u'")
# df = pd.read_sql('select * from defaultdb', con=engine)
# print(df)
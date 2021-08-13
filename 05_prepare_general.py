import pandas as pd
import psycopg2 as pg
import psycopg2
from sqlalchemy import create_engine
from functions_db.db_prepare_general import db_prepare_general

db_prepare_general()
import toml
data_toml = toml.load("config.toml")

site_url = data_toml['site_url']
n_keywords = data_toml['n_keywords']

host = data_toml['host']
database = data_toml['database']
user = data_toml['user']
password = data_toml['password']
port= data_toml['port']
postgre_complete_url = data_toml['postgre_complete_url']

# create row in database
print(site_url)
print(type(site_url))

#
# Creazione DF generale
engine = create_engine(postgre_complete_url)
df_general = pd.read_sql_query(f"SELECT * FROM wtforecastgeneral WHERE site = '{site_url}';",con=engine)
print(df_general)
domain_already_exist = df_general.loc[df_general['site'].str.contains(f"{site_url}", case=False)]
print(domain_already_exist)
if domain_already_exist.empty:
     # insert code here
    conn = psycopg2.connect(
        database=database, user=user, password=password, host=host, port=port
    )
    cur = conn.cursor()
    cur.execute("""INSERT INTO wtforecastgeneral(site) VALUES(%s) ;""", (site_url,))
    #print(cur.query)
    conn.commit()
    conn.close()

#
# Creazione DF generale
engine = create_engine(postgre_complete_url)
df_general = pd.read_sql_query(f"SELECT * FROM wtforecast WHERE site = '{site_url}' AND gt_dates IS NOT NULL;",con=engine)
#st.write(df_general[:20])

#
# Trend
#df_trend = df_trend.replace(np.nan, 'Unknown')
df_trend = df_general.copy()
#st.write(df_trend[:20])
df_trend = df_trend.dropna(subset=['gt_dates'])
#st.write(df_trend[:20])

gt_dates_trend = df_trend['gt_dates']

df_to_graph = None
for index, row in df_trend.iterrows():
    gt_dates_trend = df_trend['gt_dates']
    print(row['keyword'], row['gt_dates'])
    df_dit = pd.DataFrame.from_dict(row['gt_dates'], orient='columns')

    df_dit['date'] = pd.to_datetime(df_dit['date'], unit='ms')
    df_dit = df_dit.T

    new_header = df_dit.iloc[0] #grab the first row for the header
    df_dit = df_dit[1:] #take the data less the header row
    df_dit.columns = new_header #set the header row as the df header

    if df_to_graph is not None:
        df_to_graph = pd.concat([df_to_graph, df_dit], ignore_index=True)
    else:
        df_to_graph = df_dit.copy()

#print(df_to_graph)
total_trend = df_to_graph.sum()
total_trend = total_trend.to_frame()
#total_trend = total_trend.T
total_trend.reset_index(inplace=True)
#total_trend

total_trend.reset_index(inplace=True)
del total_trend['index']
#total_trend
total_trend = total_trend.rename(columns={'date': 'index',0: 'trend' })
total_trend['index'] = total_trend['index'].astype('datetime64[ns]')
total_trend['trend'] = total_trend['trend'].astype('float')

dict_to_sql = total_trend.to_json(orient = 'records')
print(dict_to_sql)

conn = psycopg2.connect(
    database=database, user=user, password=password, host=host, port=port
)
cur = conn.cursor()
cur.execute(f"UPDATE wtforecastgeneral SET gt_dates = '{dict_to_sql}' WHERE site = %s", (site_url,))
#print(cur.query)
conn.commit()
conn.close()




#
# Predict
df_predict = df_general.copy()
df_predict = df_predict.dropna(subset=['forecast_dates'])

gt_dates_predict = df_predict['forecast_dates']
#gt_dates_predict

df_to_graph = None
for index, row in df_predict.iterrows():
    gt_dates_predict = df_predict['forecast_dates']
    print(row['keyword'], row['gt_dates'])
    df_dit = pd.DataFrame.from_dict(row['forecast_dates'], orient='columns')

    df_dit['date'] = pd.to_datetime(df_dit['date'], unit='ms')
    df_dit = df_dit.T

    new_header = df_dit.iloc[0] #grab the first row for the header
    df_dit = df_dit[1:] #take the data less the header row
    df_dit.columns = new_header #set the header row as the df header

    if df_to_graph is not None:
        df_to_graph = pd.concat([df_to_graph, df_dit], ignore_index=True)
    else:
        df_to_graph = df_dit.copy()

total_predict = df_to_graph.sum()
total_predict = total_predict.to_frame()
#total_predict = total_predict.T
total_predict.reset_index(inplace=True)
#total_predict

total_predict.reset_index(inplace=True)
del total_predict['index']
#total_predict
total_predict = total_predict.rename(columns={'date': 'index',0: 'predict' })
total_predict['index'] = total_predict['index'].astype('datetime64[ns]')
total_predict['predict'] = total_predict['predict'].astype('float')

dict_to_sql = total_predict.to_json(orient = 'records')
print(dict_to_sql)

conn = psycopg2.connect(
    database=database, user=user, password=password, host=host, port=port
)
cur = conn.cursor()
cur.execute(f"UPDATE wtforecastgeneral SET forecast_dates = '{dict_to_sql}' WHERE site = %s", (site_url,))
#print(cur.query)
conn.commit()
conn.close()



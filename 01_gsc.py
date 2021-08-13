import pandas as pd
import os, time, subprocess, pickle, psycopg2
from datetime import datetime, timedelta
from google_auth_oauthlib.flow import InstalledAppFlow
from sqlalchemy import create_engine
import psycopg2 as pg
from apiclient.discovery import build
from functions_db.db_prepare_table import db_prepare_table


import toml
data_toml = toml.load("config.toml")

db_prepare_table()

timestr = time.strftime("%Y%m%d-%H%M%S")
SITE_URL = data_toml['site_url']
print(SITE_URL)
print(timestr)
site = data_toml['site_url']
host = data_toml['host']
database = data_toml['database']
user = data_toml['user']
password = data_toml['password']
port= data_toml['port']
postgre_complete_url = data_toml['postgre_complete_url']
gsc_range_aggregate = data_toml['gsc_range_aggregate']
gsc_country_filter = data_toml['gsc_country_filter']

OAUTH_SCOPE = ('https://www.googleapis.com/auth/webmasters.readonly', 'https://www.googleapis.com/auth/webmasters')

REDIRECT_URI = 'urn:ietf:wg:oauth:2.0:oob'

try:
 credentials = pickle.load(open("config_file/credentials.pickle", "rb"))
except (OSError, IOError) as e:
    gsc_credential_file = data_toml['gsc_credential_file']
    flow = InstalledAppFlow.from_client_secrets_file(gsc_credential_file, scopes=OAUTH_SCOPE)
    credentials = flow.run_console()
    pickle.dump(credentials, open("config_file/credentials.pickle", "wb"))

# Connect to Search Console Service using the credentials
webmasters_service = build('webmasters', 'v3', credentials=credentials)

maxRows = 25000
i = 0

output_rows = []

start_date = data_toml['gsc_start_date']
end_date = data_toml['gsc_end_date']
start_date = datetime.now() + timedelta(days=start_date)
end_date = datetime.now() + timedelta(days=end_date)

def date_range(start_date, end_date, delta=timedelta(days = gsc_range_aggregate)):

    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date += delta

for date in date_range(start_date, end_date):
    date = date.strftime("%Y-%m-%d")
    print(date)
    gsc_pause = data_toml['gsc_pause']
    print(f'pausa {gsc_pause} secondi')
    time.sleep(gsc_pause)
    i = 0
    while True:

        request = {
            'startDate' : date,
            'endDate' : date,
            'dimensions' : ["query","page","country","device"],
            "searchType": "Web",
            'rowLimit' : maxRows,
            'startRow' : i * maxRows
        }

        response = webmasters_service.searchanalytics().query(siteUrl = SITE_URL, body=request).execute()
        print()
        if response is None:
            print("there is no response")
            break
        if 'rows' not in response:
            print("row not in response")
            break
        else:
            for row in response['rows']:
                keyword = row['keys'][0]
                page = row['keys'][1]
                country = row['keys'][2]
                device = row['keys'][3]
                output_row = [date, keyword, page, country, device, row['clicks'], row['impressions'], row['ctr'], row['position']]
                output_rows.append(output_row)
            i = i + 1

df = pd.DataFrame(output_rows, columns=['date','query','page', 'country', 'device', 'clicks', 'impressions', 'ctr', 'avg_position'])

#------------------------------------------------------------------------------------------------------------------

df2 = df.copy()

df['gsc_sum_imp'] = df.groupby(['query','page'])['impressions'].transform('sum')
del df['impressions']
df['gsc_sum_clic'] = df.groupby(['query','page'])['clicks'].transform('sum')
del df['clicks']
df = df[df.country == gsc_country_filter]
df = df.drop(columns=['country'])
colonne = list(df)
#print(colonne)
df = df.sort_values(['query', 'date',],ascending=[False, False])
count_duplicate_query = df.pivot_table(index=['query'], aggfunc='size')
#print(count_duplicate_query)
df.drop_duplicates(subset ="query", keep = 'first', inplace = True)
df.sort_values("gsc_sum_imp", inplace = True, ascending = False)

#Rimozione numero di impression basso
gsc_filter_sum_imp = data_toml['gsc_filter_sum_imp']
df = df[df.gsc_sum_imp > gsc_filter_sum_imp]

# ----------------------------------------------------------------------------------

postgre_complete_url = data_toml['postgre_complete_url']
engine = create_engine(postgre_complete_url)
#df = pd.read_csv('02_google_search_console.csv', sep=';')

df['site'] = SITE_URL
df['gt_parsed'] = 0
df['ga_parsed'] = 0
df['last_trend'] = None
df['last_forecast'] = None
df['gt_accuracy'] = None

df = df[["site", "query", "gt_parsed", "ga_parsed", "last_trend", "last_forecast", "gt_accuracy", "date", "device", "ctr", "avg_position", "gsc_sum_imp", "gsc_sum_clic", "page"]]
df = df.rename(columns={'query': 'keyword', 'date': 'gsc_date'})
df = df.rename(columns={'device': 'gsc_device', 'ctr': 'gsc_ctr'})
df = df.rename(columns={'avg_position': 'gsc_avg_pos', 'page': 'gsc_page'})
    

#Cancellazione righe per sito
conn = psycopg2.connect(
    database=database, user=user, password=password, host=host, port= port
)
cur = conn.cursor()
cur.execute(f"DELETE FROM wtforecast WHERE site = %s;", (site,))
#print(cur.query)
conn.commit()
conn.close()


# Push su db
df.to_sql('wtforecast', engine, index=False, if_exists="append")
#print(df)

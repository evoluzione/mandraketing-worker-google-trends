import sqlite3, datetime, psycopg2, time, re, json
from pytrends.request import TrendReq
from datetime import timedelta, datetime, date
import pandas as pd
#from pytrends import *
import psycopg2 as pg
from sqlalchemy import create_engine
from functions_db.sqlite_trends_prepare import create_db_and_folder

import toml
data_toml = toml.load("config.toml")

create_db_and_folder()

site_url = data_toml['site_url']
file_proxies = data_toml['file_proxies']
db_name_proxy = data_toml['db_name_proxy']
site = data_toml['site_url']
host = data_toml['host']
database = data_toml['database']
user = data_toml['user']
password = data_toml['password']
port= data_toml['port']
postgre_complete_url = data_toml['postgre_complete_url']
week_in_year = data_toml['week_in_year']

def select_proxy():
    conn = sqlite3.connect(db_name_proxy)
    c = conn.cursor()
    data = pd.read_sql_query(
        "SELECT PROXY FROM PROXY_LIST WHERE TIME = ( SELECT MIN(TIME) FROM PROXY_LIST);", conn)
    global proxy
    proxy = (data['PROXY'].iat[0])
    print(f'---------------------Request IP is {proxy}')
    timestr_now = str(datetime.now())
    timestr = datetime.fromisoformat(timestr_now).timestamp()
    c.execute("Update PROXY_LIST set TIME = ? where PROXY = ?", (timestr, proxy))
    conn.commit()
    conn.close()

def select_keyword():
    conn = psycopg2.connect(
        database=database, user=user, password=password, host=host, port= port
    )
    cur = conn.cursor()
    cur.execute("SELECT keyword FROM wtforecast WHERE site = %s AND gt_parsed = 0;", (site_url,))
    data = cur.fetchall()
    #print(type(data))
    global keyword
    keyword = data[0]
    keyword = ''.join(keyword)
    global single_kw_list
    single_kw_list = []
    single_kw_list.append(keyword)
    print(single_kw_list)
    cur.execute("Update wtforecast set gt_parsed = 1 where KEYWORD = %s AND site = %s", (keyword, site_url,))
    conn.commit()
    conn.close()

def check_record_db():
    conn = psycopg2.connect(
        database=database, user=user, password=password, host=host, port= port
    )
    cur = conn.cursor()
    print(site_url)
    #print("SELECT keyword FROM wtforecast WHERE site = %s AND gt_parsed = 0;",(site_url))
    cur.execute("SELECT keyword FROM wtforecast WHERE site = %s AND gt_parsed = 0;",(site_url,))
    data = cur.fetchall()
    global numbers_kw
    numbers_kw = len(data)
    print(numbers_kw)
    conn.commit()
    conn.close()

check_record_db()

while numbers_kw != 0:
    select_keyword()
    select_proxy()
    print(f'keyword ------- {keyword}')
    single_search=keyword

    dataset = []
    proxies = []

    proxies.append(proxy)
    #print(proxies)
    kw_list = []
    kw_list.append(keyword)
    #print('keyword list =======')
    #print(kw_list)

    try:
        #pytrends =TrendReq(hl='it-IT', tz=360, timeout=(10,25), proxies=proxies, retries=5, backoff_factor=0.1)
        pytrends =TrendReq(hl='it-IT', tz=360, timeout=(10,25), retries=5, backoff_factor=0.1)
        #print('pytrend ok')
    except:
        conn = psycopg2.connect(database=database, user=user, password=password, host=host, port= port)
        cur = conn.cursor()
        cur.execute("UPDATE wtforecast SET gt_parsed = 0 WHERE keyword = %s AND site = %s", (keyword, site_url,))
        conn.commit()
        conn.close()

    pytrends.build_payload(kw_list, cat=0, timeframe='today 5-y', geo='IT', gprop='')
    data = pytrends.interest_over_time()
    #print(data)
    if not data.empty:
        # data.drop(labels=['isPartial'], axis='columns')
        data = data.drop(labels=['isPartial'],axis='columns')
        dataset.append(data)
        check_record_db()
        print('-------------------------dataframe from pytrends OK')
    else:
        print('-------------------------dataframe from pytrends empty')
        pass

    # Calcolo accuratezza
    #print(data)
    accurancy = data.loc[data[f'{keyword}'] == 0, f'{keyword}'].count()
    #print('-------------------------ACCURANCY')
    #print(accurancy)
    conn = psycopg2.connect(
        database=database, user=user, password=password, host=host, port= port
    )
    cur = conn.cursor()
    cur.execute(f"UPDATE wtforecast SET gt_accuracy = {accurancy} WHERE site = %s AND keyword = %s;", (site_url, keyword,))
    #print(cur.query)
    conn.commit()
    conn.close()


    #calcolo ultimo Trend anno da 1 a 100
    #print(data)
    data_trend_1_100 = data[-week_in_year:]
    #print(data_trend_1_100)
    a, b = 0, 100
    x, y = data_trend_1_100[f'{keyword}'].min(), data_trend_1_100[f'{keyword}'].max()
    data_trend_1_100['SIZE'] = (data_trend_1_100[f'{keyword}'] - x) / (y - x) * (b - a) + a
    data_trend_1_100.drop([f'{keyword}'], axis=1, inplace=True)
    data_trend_1_100.rename(columns={'SIZE':f'{keyword}'}, inplace=True)
    #print(data_trend_1_100)
    last_trend_1 = data_trend_1_100[f'{keyword}'].iloc[-1]
    last_trend_2 = data_trend_1_100[f'{keyword}'].iloc[-2]
    last_trend = max(last_trend_1, last_trend_2)
    #print(last_trend)
    conn = psycopg2.connect(
        database=database, user=user, password=password, host=host, port= port
    )
    cur = conn.cursor()
    cur.execute(f"UPDATE wtforecast SET last_trend = {last_trend} WHERE site = %s AND keyword = %s;", (site_url, keyword,))
    #print(cur.query)
    conn.commit()
    conn.close()


    try:
        result = pd.concat(dataset, axis=1)
        result.reset_index(inplace=True)
        #print(result)

        dict_to_sql = result.to_json(orient = 'records')
        #print(dict_to_sql)

        conn = psycopg2.connect(
            database=database, user=user, password=password, host=host, port= port
        )
        cur = conn.cursor()
        cur.execute(f"UPDATE wtforecast SET gt_dates = '{dict_to_sql}' WHERE site = %s AND keyword = %s;", (site_url, keyword,))
        #print(cur.query)
        conn.commit()
        conn.close()
        check_record_db()


    
    except:
        check_record_db()
        pass
    

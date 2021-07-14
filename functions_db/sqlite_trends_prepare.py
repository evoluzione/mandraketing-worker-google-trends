import os, time, sqlite3, random
import pandas as pd
from pandas import DataFrame
from datetime import datetime
from os import path
import toml
data_toml = toml.load("config.toml")

def create_db_and_folder():
    teporary_file = data_toml['teporary_file']
    file_proxies = data_toml['file_proxies']
    db_name_proxy = data_toml['db_name_proxy']

    #creazione Cartelle
    if not os.path.exists(teporary_file):
        os.makedirs(teporary_file)


    #
    # Creazione dataframe proxy
    dataframe = pd.read_csv(file_proxies, encoding='utf-8', header=None)
    #timestr = time.strftime('%Y-%m-%d %H:%M:%S')
    timestr_now = str(datetime.now())
    #print(timestr_now)
    timestr = datetime.fromisoformat(timestr_now).timestamp()
    #print(timestr)
    dataframe['TIME'] = timestr
    dataframe.columns = ['PROXY','TIME']
    #print(dataframe)


    #
    # Creazione DB da Dataframe PROXY
    check_db = path.exists(db_name_proxy)
    #print(check_db)
    if check_db == False:
        conn = sqlite3.connect(db_name_proxy,detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        c = conn.cursor()
        c.execute('CREATE TABLE PROXY_LIST (PROXY text, TIME timestamp)')
        conn.commit()
        df = DataFrame(dataframe, columns= ['PROXY','TIME'])
        df.to_sql('PROXY_LIST', conn, if_exists='replace', index = True)
        c.execute('''  
        SELECT * FROM PROXY_LIST
                ''')
        # for row in c.fetchall():
        #     print(row)
        del df
        del dataframe
        conn.close()
    else:
        print('DB già presente PROXY')



#create_db_and_folder()
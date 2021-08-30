import psutil
import os
from sqlalchemy import create_engine
import psycopg2
import pandas as pd
from functions_db.sqlite_trends_prepare import create_db_and_folder
import subprocess

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

max_num_processi = 3
pausa_1 = 1
pausa_2 = 2
pausa_3 = 3
file_da_eseguire1 = '02_gt_0.py'
file_da_eseguire2 = '02_gt_1.py'
file_da_eseguire3 = '02_gt_2.py'


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

def is_running(script):
    for q in psutil.process_iter():
        if q.name().startswith('python'):
            if len(q.cmdline())>1 and script in q.cmdline()[1] and q.pid !=os.getpid():
                print("'{}' Process is already running".format(script))
                return True

    return False


check_record_db()
max_num_processi_list = list(range(1, max_num_processi+1))
while numbers_kw != 0:
    print('---------------------------------')
    print(max_num_processi_list)
    print(type(max_num_processi_list))
    for i in max_num_processi_list:
        print(i)
        check_record_db()
        if numbers_kw != 0:
            if not is_running(f'02_gt_{i}'):
                cmd = subprocess.Popen(['python3',f'02_gt_{i}.py'])
                cmd.communicate()
                print("file_da_eseguire")
                check_record_db()



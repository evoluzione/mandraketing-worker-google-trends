import subprocess as subp
import subprocess
import pandas as pd
import time, os, datetime, subprocess, psutil, sqlite3

import psycopg2
from datetime import timedelta, datetime, date
import pandas as pd
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

#Get a list of all processes with a certain name
def get_pid(name):
    return list(map(int,subp.check_output(["pidof", "-c", name]).split()))

max_num_processi = 1
pausa_1 = 1
pausa_2 = 2
pausa_3 = 3
file_da_eseguire = '02_gt_0.py'
file_da_eseguire1 = '02_gt_1.py'
file_da_eseguire2 = '02_gt_2.py'


a = 1
i = 0
print("al mio segnale scatenate i processi :D")
time.sleep(pausa_1)


#Avvio primo terminale
os.system(f"python3 {file_da_eseguire}")

check_record_db()
while numbers_kw != 0:
    #Get a list of all pids for python3 processes
    python_pids = get_pid('python3')
    python_pids1 = get_pid('python3')
    python_pids2 = get_pid('python3')
    #Get a list of all pids for processes with "main.py" argument
    try:
        main_py_pids = list(map(int,subp.check_output(["pgrep", "-f", f"{file_da_eseguire}"]).split()))
        main_py_pids = list(map(int,subp.check_output(["pgrep", "-f", f"{file_da_eseguire1}"]).split()))
        main_py_pids = list(map(int,subp.check_output(["pgrep", "-f", f"{file_da_eseguire2}"]).split()))
        check_record_db()
    except subprocess.CalledProcessError as e:
        print('nessun processo attivo')
        os.system(f"python3 {file_da_eseguire}")
        print('YYYYYYYY-----processo eseguito')
        time.sleep(pausa_1)
        main_py_pids = list(map(int,subp.check_output(["pgrep", "-f", f"{file_da_eseguire}"]).split()))

        print('nessun processo attivo')
        os.system(f"python3 {file_da_eseguire1}")
        print('YYYYYYYY-----processo eseguito')
        time.sleep(pausa_1)
        main_py_pids1 = list(map(int,subp.check_output(["pgrep", "-f", f"{file_da_eseguire1}"]).split()))

        print('nessun processo attivo')
        os.system(f"python3 {file_da_eseguire2}")
        print('YYYYYYYY-----processo eseguito')
        time.sleep(pausa_1)
        main_py_pids2 = list(map(int,subp.check_output(["pgrep", "-f", f"{file_da_eseguire2}"]).split()))
        check_record_db()
    
    #main_py_pids = list(map(int,subp.check_output(["pgrep", "-f", f"{file_da_eseguire}"]).split()))
    python_main_py_pid = set(python_pids).intersection(main_py_pids)
    #print(python_pids)
    print(main_py_pids)
    #print(python_main_py_pid)
    number_of_python_process = len(main_py_pids)
    print(number_of_python_process)
    check_record_db()

    #main_py_pids = list(map(int,subp.check_output(["pgrep", "-f", f"{file_da_eseguire}"]).split()))
    python_main_py_pid1 = set(python_pids1).intersection(main_py_pids1)
    #print(python_pids)
    print(main_py_pids1)
    #print(python_main_py_pid)
    number_of_python_process1 = len(main_py_pids1)
    print(number_of_python_process1)
    check_record_db()

    #main_py_pids = list(map(int,subp.check_output(["pgrep", "-f", f"{file_da_eseguire}"]).split()))
    python_main_py_pid2 = set(python_pids2).intersection(main_py_pids2)
    #print(python_pids)
    print(main_py_pids2)
    #print(python_main_py_pid)
    number_of_python_process2 = len(main_py_pids2)
    print(number_of_python_process2)
    check_record_db()

    time.sleep(pausa_1)

    if number_of_python_process < max_num_processi:
        os.system(f"python3 {file_da_eseguire}")
        print('YYYYYYYY-----processo eseguito')
        time.sleep(pausa_1)
        check_record_db()
    else:
        print('XXXXXXX----raggiunto il numero massimo di processi')
        time.sleep(pausa_3)
        check_record_db()

    if number_of_python_process1 < max_num_processi:
        os.system(f"python3 {file_da_eseguire1}")
        print('YYYYYYYY-----processo eseguito')
        time.sleep(pausa_1)
        check_record_db()
    else:
        print('XXXXXXX----raggiunto il numero massimo di processi')
        time.sleep(pausa_3)
        check_record_db()

    if number_of_python_process2 < max_num_processi:
        os.system(f"python3 {file_da_eseguire2}")
        print('YYYYYYYY-----processo eseguito')
        time.sleep(pausa_1)
        check_record_db()
    else:
        print('XXXXXXX----raggiunto il numero massimo di processi')
        time.sleep(pausa_3)
        check_record_db()
    

    check_record_db()
    print('-----------------------FINITO')



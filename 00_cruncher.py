import subprocess as subp
import subprocess


def create_all():
    file = 'functions_db/db_prepare_table.py'
    print(f'{file}')
    cmd = subprocess.Popen(['python3',f"{file}"])
    cmd.communicate()    

def delete_all():
    file = 'functions_db/db_delete_all_table.py'
    print(f'{file}')
    cmd = subprocess.Popen(['python3',f"{file}"])
    cmd.communicate()    

def file_1():
    file = '01_gsc.py'
    print(f'{file}')
    cmd = subprocess.Popen(['python3',f"{file}"])
    cmd.communicate()

def file_2():
    file = '02_cruncher_gt.py'
    print(f'{file}')
    cmd = subprocess.Popen(['python3',f"{file}"])
    cmd.communicate()

def file_3():
    file = '03_gads.py'
    cmd = subprocess.Popen(['python3',f"{file}"])
    cmd.communicate()

def file_4():
    file = '04_predict.py'
    cmd = subprocess.Popen(['python3',f"{file}"])
    cmd.communicate()


def file_5():
    file = '05_prepare_general.py'
    cmd = subprocess.Popen(['python3',f"{file}"])
    cmd.communicate()

def file_6():
    file = '06_prepare_details.py'
    cmd = subprocess.Popen(['python3',f"{file}"])
    cmd.communicate()


#delete_all()
create_all()

file_1()
file_2()
file_3()
file_4()
file_5()
file_6()

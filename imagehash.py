#!/usr/bin/env python
import os
import pandas as pd
import fnmatch
import hashlib
import time
import argparse
from datetime import date
import pathlib
from pathlib import Path

ROOT = r'C:\datafile\imagefolder'


RESULT_FILE = './imagehash.csv'


CONNECTION_HOST = 'localhost'
CONNECTION_DATABASE ='mytest'
CONNECTION_PASSWORD ='Kahu@user'
CONNECTION_USER ='splitter'

## read .jpg files in all subdir and convert them into hashcode, output into imagehash.csv
def image_hash(root):
    pattern='*.jpg'
    lst=[]
    cols = ['name','path','hashcode']
    for path1 in Path(root).iterdir():
        if path1.is_dir():
            for path, subdirs, files in os.walk (path1):
                for name in files:
                    if fnmatch.fnmatch(name,pattern):         
                        full_path = os.path.join(path, name)
                        sub_path = os.path.relpath(path, root)
                        with open(full_path,"r+b") as f:
                            bytes = f.read() # read entire file as bytes
                            readable_hash = hashlib.sha256(bytes).hexdigest(); # Sha256 code conversion
                            lst.append([name,sub_path,readable_hash])
    importlen= (len(lst))
    df = pd.DataFrame(lst, columns=cols)
    df.to_csv('imagehash.csv',index=False)
    print("file output to imagehash.csv,length:",importlen)

## compare the latest updated hashfile with the last hashfile, find records with the same Hashcode but file name different
def hash_compare(tableName,compareName,cursor,conn):                
        cursor.execute(f"SELECT count(*) FROM {tableName} AS T INNER JOIN {compareName} AS C ON T.hashcode = C.hashcode WHERE T.name <> C.name;")
        global importerr
        global list_errfile
        importerr = cursor.fetchall()
        print('Hashcode same but file name different:',importerr)
        sql_query = pd.read_sql_query(f"SELECT * FROM {tableName} AS T INNER JOIN {compareName} AS C ON T.hashcode = C.hashcode WHERE T.name <> C.name;",conn) 
        df1 = pd.DataFrame(sql_query)
        list_errfile = df1.iloc[:,0].tolist()
        df1.to_csv (r'error.csv', index = False) 

def log_entry(output,err,cursor,conn):
## update logfile username, appname ,datetime, OUTPUT
    import getpass as gt
    from datetime import datetime
    getuser = gt.getuser()
    getapp = os.path.basename(__file__)
    now = datetime.now()
    stmt1 = "SHOW TABLES LIKE 'logfile'"
    cursor.execute(stmt1)
    filecheck = cursor.fetchone()
    if filecheck:
        cursor.execute("INSERT INTO mytest.logfile (username, appname, last_update, output,error) VALUES(%s,%s,%s,%s,%s)",(getuser, getapp,now,output,err))
        conn.commit()
    else:
        cursor.execute("CREATE TABLE mytest.logfile (username VARCHAR(50), appname VARCHAR(20),last_update DATETIME, output VARCHAR(200),error TEXT(500))")
        print("Table logfile is created....")
        cursor.execute("INSERT INTO logfile (username, appname, last_update, output,error) VALUES(%s,%s,%s,%s,%s)",(getuser, getapp,now,output,err))
        conn.commit() 


def db_upload():
## upload data of imagehash.csv onto database,default schema name is mytest, table imagehash with suffix 1,2,3... for each update, difference of the latest updated 2 tables compared
    image_data = pd.read_csv(RESULT_FILE,index_col=False, delimiter = ',')
    import mysql.connector as msql
    from mysql.connector import Error
    try:
        conn = msql.connect(host=CONNECTION_HOST, database=CONNECTION_DATABASE, user=CONNECTION_USER, password=CONNECTION_PASSWORD)
        if conn.is_connected():
            cursor = conn.cursor(buffered = True)
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)
            stmt = "SHOW TABLES LIKE'imagehash%'"
            cursor.execute(stmt)
            testexist = cursor.fetchone()
            if testexist:
                #print("imagehash1 already exist")
                cursor.execute("""SELECT count(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = "mytest" AND TABLE_NAME LIKE 'imagehash%'; """)
                N_records= cursor.fetchone()
                num_record = 1 + N_records[0]
                print(num_record)
                tableName = 'imagehash' + str(num_record)
                cursor.execute("CREATE TABLE mytest." + tableName + "(name VARCHAR(300) NOT NULL, path VARCHAR(200),hashcode CHAR(64))")
                cursor.execute("ALTER TABLE imagehash ADD primary key(name,path);")
                print("Table",tableName,"is created....Primary key(name+path)")
## upload data with load data infile            
                cursor.execute("LOAD DATA INFILE" + RESULT_FILE + "INTO TABLE mytest." + tableName + "FIELDS TERMINATED BY ',' IGNORE 1 ROWS;")
                compareName = 'imagehash' + str(N_records[0])
                #for comparing two most recent hashing tables, file output as error.csv
                hash_compare(tableName,compareName,cursor,conn)
            else:      
                cursor.execute("CREATE TABLE mytest.imagehash1(name VARCHAR(300) NOT NULL, path VARCHAR(200),hashcode CHAR(64))")
                cursor.execute("ALTER TABLE imagehash1 ADD primary key(name,path);")
                cursor.execute("LOAD DATA INFILE" + RESULT_FILE + "INTO TABLE imagehash1 FIELDS TERMINATED BY ',' IGNORE 1 ROWS;")
                print("Table imagehash1 is created....Primary key(name,path)")                    
                print("Total records inserted:", len(image_data))
            output_info = "total records inserted:" + str(len(image_data))
            output_err = "redundant or wrong image filenames:" #+ str(importerr) + str(list_errfile)
            log_entry(output_info,output_err,cursor,conn)
    except Error as e:
                print("Error while connecting to MySQL", e)

# Split filename into label, data_type,global_patient_id,leision_id and output as imagehash_split.csv
def filesplit(resultfile):
    df = pd.read_csv(resultfile,encoding='UTF-8',index_col=False, delimiter = ',')
    df.dropna(inplace = True)
    split_name = df['name'].str.split("\\.",n = 3, expand = True)
    split_path = df['path'].str.split("\\",n = 1, expand = True)
    df["label"]= split_path[0]
    df["data_type"]= split_path[1]
    df["global_pat_id"]=split_name[0]
    df["leision_id"]=split_name[1]
    df["file_name"]=split_name[3]
    df["hash"] = df['hashcode']
    df.drop(columns =["name","path","hashcode"], inplace = True)
    df.to_csv(r"imagehash_split.csv",index=False)

# --test for checking total timespan, --directory with validity checking of input directory,default directory ROOT
def main(arg):
    
    if arg.test:
        start = time.time()
        image_hash(ROOT)        
        db_upload()
        end = time.time()
        print("total time:", end-start)
    elif pathlib.Path(arg.directory) :
        NEW_ROOT = str(arg.directory)
        image_hash(NEW_ROOT)
        db_upload()
    elif arg.split_by_file:
        filesplit(RESULT_FILE)
    else:
        print("argument is required")



if __name__ == "__main__":
    parser = argparse.ArgumentParser( description="parse image .jpg into 64 hashing codes and output to Imagehash.csv")
    parser.add_argument('--directory',type=pathlib.Path,help="python imagehash.py --directory '/mnt/data/Molemap_Images_d5.1'", required=False)
    parser.add_argument('--test',default=False,action="store_true",help="Testing mode")
    parser.add_argument('--split_by_file', default=False, action='store_true', help='split data by patient id')  
    main(parser.parse_args())



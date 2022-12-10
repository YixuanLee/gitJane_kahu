#!/usr/bin/env python
import os
from doctest import OutputChecker
from unittest import result
import pandas as pd
import numpy as np
import os
import argparse
import pathlib
import datetime
from datetime import date
import time

# using today() to get current date
datetoday = date.today()
OLD_FILE = '/.../old.csv'# can be with folder /url / web link
NEW_FILE = '/.../new.csv'

RESULT_FILE = './result.csv'


CONNECTION_HOST = 'localhost'
CONNECTION_DATABASE ='mytest'
CONNECTION_PASSWORD ='test@kahu'
CONNECTION_USER ='testuser'

# database schema default-name is mytest in line 61 'CREATE TABLE mytest.result...'

def insert_data(mydata, cursor, conn ):
    for i,row in mydata.iterrows():
        sql = "INSERT INTO mytest.result VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor.execute(sql, tuple(row))
        conn.commit ()        
    print("Total records inserted:", len(mydata))

# split df into columns: photo name, L0,L1,L2 labels and insert column date
def split_file(df):
    split_name = df['name'].str.split("/", n=2, expand = True)
    split_lable = df['label'].str.split(':',n=3, expand = True)
    df['photo'] = split_name[2]
    df['L0']=split_lable[0]
    df['L1']=split_lable[1]
    df['L2']=split_lable[2]
    return(df)

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
        cursor.execute("CREATE TABLE mytest.logfile (username VARCHAR(50), appname VARCHAR(20),last_update DATETIME, output VARCHAR(400),error TEXT(500))")
        print("Table logfile is created....")
        cursor.execute("INSERT INTO logfile (username, appname, last_update, output,error) VALUES(%s,%s,%s,%s,%s)",(getuser, getapp,now,output,err))
        conn.commit() 

# checking if files exit, file format and import result.csv in database, schema name:mytest
def process_files(old_file, new_file):
    global result
    if os.path.isfile(old_file) and os.path.isfile(new_file): # checking if files exist under your subdirectory
        label =['name','label','is_train','data_type']
        df = pd.read_csv(old_file, encoding ='UTF-8', index_col= False,delimiter =',')
        df1 = pd.read_csv(new_file, encoding ='UTF-8', index_col= False,delimiter =',')
        df_col = list(df.columns)
        df1_col=list(df1.columns)
        if df_col == df1_col == label: # checking if file format is correct
            print('Files exist, headers are correct','old.csv has records:',len(df),'new.csv has records:',len(df1))
# split file and do comparison to get records deleted, added, label change
            old = split_file(df)
            new = split_file(df1)
            print(list(old.columns), list(new.columns))
            # merge old file and new file on 'label' + 'photo', records occurs only in the old files 
            Only_old = pd.merge(old,new,indicator = True, how='outer', on=['label','photo']).loc[lambda v:v['_merge'] == 'left_only']
            print(list(Only_old.columns))
            # records either newly added photos or photos with label change
            Only_new = pd.merge(old,new,indicator = True, how='outer', on=['label','photo']).loc[lambda v:v['_merge'] == 'right_only']
            print(Only_new)
            # actual deleted records
            pd.set_option('mode.chained_assignment', None) 
            R_delete = Only_old[~Only_old['photo'].apply(tuple,1).isin(Only_new['photo'].apply(tuple,1))]
            if len(R_delete) !=  0:
                R_delete.loc[:,'status'] ='delete'
                R_delete['date'] = datetoday
            # actual new image records
            R_new = Only_new[~Only_new['photo'].apply(tuple,1).isin(old['photo'].apply(tuple,1))]
            if len(R_new) !=  0:
                R_new.loc[:,'status'] ='new'
                R_new['date'] = datetoday
            #tracking label changes
            # L0 change
            R_L0 = Only_new[Only_new[['photo','L1_y','L2_y']].apply(tuple,1).isin(old[['photo','L1','L2']].apply(tuple,1))]
            if len(R_L0) !=  0:
                R_L0['status'] ='L0change'
            # L0&L1 change
            R_L0L1 = Only_new[~Only_new['L0_y'].isin(old['L0']) & ~Only_new['L1_y'].isin(old['L1']) & Only_new[['photo','L2_y']].apply(tuple,1).isin(old[['photo','L2']].apply(tuple,1))]
            if len(R_L0L1) !=  0:
                R_L0L1['status'] ='L0L1change'
            # L0&L1&L2 change
            R_L0L1L2 = Only_new[~Only_new['L0_y'].isin(old['L0']) & ~Only_new['L1_y'].isin(old['L1']) & ~Only_new['L2_y'].isin(old['L2']) & Only_new['photo'].apply(tuple,1).isin(old['photo'].apply(tuple,1))]
            if len(R_L0L1L2) !=  0:
                R_L0L1L2['status'] ='L0L1L2change'
            # L1 change
            R_L1 = Only_new[Only_new[['photo','L0_y','L2_y']].apply(tuple,1).isin(old[['photo','L0','L2']].apply(tuple,1))]
            if len(R_L1) !=  0:
                R_L1['status'] ='L1change'
            # L1&L2 change
            R_L1L2 = Only_new[~Only_new['L1_y'].isin(old['L1']) & ~Only_new['L2_y'].isin(old['L2']) & Only_new[['photo','L0_y']].apply(tuple,1).isin(old[['photo','L0']].apply(tuple,1))]
            if len(R_L1L2) !=  0:
                R_L1L2['status'] ='L1L2change'
            # L2 change
            R_L2 = Only_new[Only_new[['photo','L0_y','L1_y']].apply(tuple,1).isin(old[['photo','L0','L1']].apply(tuple,1))]
            if len(R_L2) !=  0:
                R_L2['status'] ='L2change'
            # L0&L2 change
            R_L0L2 = Only_new[~Only_new['L0_y'].isin(old['L0']) & ~Only_new['L2_y'].isin(old['L2']) & Only_new[['photo','L1_y']].apply(tuple,1).isin(old[['photo','L1']].apply(tuple,1))]
            if len(R_L0L2) !=  0:
                R_L0L2['status'] ='L0L2change'
            ## concatenate all df with label change
            R_label = pd.concat([R_new,R_L0,R_L0L1,R_L0L1L2,R_L1,R_L1L2,R_L2,R_L0L2])
            if len(R_label) !=  0:
                R_label['date'] = datetoday
            ## rename column names of R_label and R_delete
            R_label.rename(columns={'L0_y': "L0", 'L1_y': "L1", 'L2_y': "L2",'is_train_y':'is_train','data_type_y':'data_type'}, errors="raise")
            R_delete.rename(columns={'L0_x': "L0", 'L1_x': "L1", 'L2_x': "L2",'is_train_x':'is_train','data_type_x':'data_type'}, errors="raise")

            R_delete = R_delete.drop(['label','name_x', 'name_y', 'is_train_y', 'data_type_y', 'L0_y', 'L1_y', 'L2_y','_merge'], axis=1)
            R_delete = R_delete[['photo','L0_x','L1_x','L2_x','status','date','is_train_x','data_type_x']]

            R_label = R_label.drop(['label','name_x', 'name_y', 'is_train_x', 'data_type_x', 'L0_x', 'L1_x', 'L2_x','_merge'], axis=1)
            R_label = R_label[['photo','L0_y','L1_y','L2_y','status','date','is_train_y','data_type_y']]
            ## final output
            Result = pd.DataFrame(np.concatenate( (R_delete.values, R_label.values), axis=0 ) )
            Result.columns = ['photo','L0','L1','L2','status','date','is_train','data_type']
            print("total records: ", len(result), len(R_delete),'deleted',len(R_new),'added',len(result)-len(R_delete)-len(R_new),'label changed')
            ## saving data
            Result.to_csv("result.csv",index=False)
            # upload data of result.csv onto database
            df_delete = len(Result[Result['status'] == 'delete']) 
            df_new = len(Result[Result['status'] == 'new'])
            df_change = len(Result)-df_delete-df_new
            global df_output, err1
            df_output = "total:" + str(len(Result)) + ",deleted:" + str(df_delete) + ",new:" + str(df_new) + ",Label change:" + str(df_change)
            print(df_output)
            err1 = 'N/A' # standard format of logfile in SQL, datasplit.py does not have error information.
            import mysql.connector as msql
            from mysql.connector import Error
            try:
                conn = msql.connect(host=CONNECTION_HOST, database=CONNECTION_DATABASE, user=CONNECTION_USER, password=CONNECTION_PASSWORD)
                if conn.is_connected():
                    cursor = conn.cursor()
                    cursor.execute("select database();")
                    record = cursor.fetchone()
                    print("You're connected to database: ", record)
                    stmt = "SHOW TABLES LIKE 'result'"
                    cursor.execute(stmt)
                    testexist = cursor.fetchone()
                    if testexist:
                        print("Table result exists....")                        
                    else:      
                        cursor.execute("CREATE TABLE mytest.result(name VARCHAR(300) NOT NULL, L0 VARCHAR(30),L1 VARCHAR(30), L2 VARCHAR(50),status VARCHAR(20),date DATE, is_train CHAR,data_type VARCHAR(10))")
                        cursor.execute("ALTER TABLE result ADD primary key(name,date);")
                        print("Table is created....Primary key(name,date)")                        
                    insert_data(Result, cursor, conn)
                log_entry(df_output,err1,cursor,conn)
            except Error as e:
                        print("Error while connecting to MySQL", e)
        else:
            print('file format is wrong')
    else:
        print("files do not exist")

def main(arg):
    if arg.test:
        process_files(OLD_FILE, NEW_FILE)
    else: 
        old_file = arg.compare[0]
        new_file = arg.compare[1]
        process_files(old_file,new_file)



if __name__ == "__main__":
    parser = argparse.ArgumentParser( description="split images files with folder lables and update the changes")    
    parser.add_argument('--compare',nargs=2, metavar=('old_file', 'new_file'), type=pathlib.Path,help="python datasplit.py --compared dir/old.csv dir/new.csv", required=False)
    parser.add_argument('--test',default=False,action="store_true",help="Testing mode",)
    main(parser.parse_args())



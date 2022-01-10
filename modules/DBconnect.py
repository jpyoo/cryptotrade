#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pymysql
from sqlalchemy import create_engine
import pandas as pd

class myDB():
    def __init__(self, sid = '', spw = ''):
        self.id = sid
        self.pw = spw
        self.host = 'database-1.cjqswaciegfj.us-east-2.rds.amazonaws.com'
        self.cdb = 'main'
        self.connect = None
        self.cursor = None
        self.engine = None
        
    def dbconnect(self):
        self.connect = pymysql.connect(host = self.host,
                                user = self.id,
                                password = self.pw,
                                database = self.cdb,
                                autocommit = True)        
        self.cursor = self.connect.cursor()
        self.engine = create_engine('mysql+pymysql://'+self.id+':'+self.pw+'@'+self.host+'/'+self.cdb).connect()
    def reconnect(self):
        try:
            self.connect.close()
        except:
            pass
        try:
            self.cursor.close()
        except:
            pass
        self.dbconnect()
        
    def drop(self):
        self.connect.close()
        self.cursor.close()

    def load_db(self, path):
        with open(path, 'r') as f:
            self.id = f.readline().strip()
            self.pw = f.readline().strip()
        return
        
    def runQuery(self, cur, high, sig):
        self.cursor.execute('Delete from t_signal')
        self.connect.commit()
        
        self.cursor.execute('Insert into t_signal (cur, high, sig) values ("' + cur + '", "'+ str(high) +'", "'+str(sig)+'");')
        self.connect.commit()

    def getSignalTable(self):
        return pd.read_sql_table('t_signal', self.engine)


#!/usr/bin/env python
# coding: utf-8

# In[1]:


# database-1
# admin
# 1234qwer
# database-1.cjqswaciegfj.us-east-2.rds.amazonaws.com
# 3306


# In[1]:


import pymysql


# In[324]:


class myDB():
      
    def __init__(self):
	self.load_db(db.key)
        dbcon = pymysql.connect(
        host = 'database-1.cjqswaciegfj.us-east-2.rds.amazonaws.com',
        user = self.id,
        password = self.pw,
        db = 'main',
        autocommit = True)
        self.connect = dbcon
        self.cursor = dbcon.cursor()
    
    def dbconnect(self):
        dbcon = pymysql.connect(
        host = 'database-1.cjqswaciegfj.us-east-2.rds.amazonaws.com',
        user = self.id,
        password = self.pw,
        db = 'main',
        autocommit = True)
        self.connect = dbcon
        self.cursor = dbcon.cursor()
    
    def reconnect(self):
        try:
            self.connect.close()
            self.connect = pymysql.connect(host = 'database-1.cjqswaciegfj.us-east-2.rds.amazonaws.com',user = self.id,password = self.pw, db = 'main')
        except:
            pass
        try:
            self.cursor.close()
            self.cursor = self.connect.cursor()
        except:
            pass
        
    def runQuery(self, cur, high):
        self.cursor.execute('Delete from t_signal')
        self.connect.commit()
        
        self.cursor.execute('Insert into t_signal (cur, high) values ("' + cur + '", "'+ str(high) +'");')
        self.connect.commit()
    def drop(self):
        self.connect.close()
        self.cursor.close()

    def load_key(self, path):
        with open(path, 'r') as f:
        self.key = f.readline().strip()
        self.secret = f.readline().strip()
        return

    def load_db(self, path):
        with open(path, 'r') as f:
        self.id = f.readline().strip()
        self.pw = f.readline().strip()
        return

# In[325]:


# db = myDB()

# db.runQuery('xxxxxx',5)

# db.cursor.execute('select * from t_signal')


# In[ ]:





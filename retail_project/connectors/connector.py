#python -m pip install mysql-connector-python
import mysql.connector
import traceback
import pandas as pd
class Connector:
    def __init__(self,server="localhost", port=3306, database="k23416_retail", username="Phuc_Diem", password="Huyndiem07@"):
        self.server=server
        self.port=port
        self.database=database
        self.username=username
        self.password=password
    def connect(self):
        try:
            self.conn = mysql.connector.connect(
                host=self.server,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password)
            return self.conn
        except:
            self.conn=None
            traceback.print_exc()
        return None

    def disConnect(self):
        if self.conn != None:
            self.conn.close()

    def queryDataset(self, sql):
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            df = pd.DataFrame(cursor.fetchall())
            if not df.empty:
                df.columns=cursor.column_names
            return df
        except:
            traceback.print_exc()
        return None
    def getTablesName(self):
        cursor = self.conn.cursor()
        cursor.execute("Show tables;")
        results=cursor.fetchall()
        tablesName=[]
        for item in results:
            tablesName.append([tableName for tableName in item][0])
        return tablesName
    def fetchone(self,sql,val):
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql,val)
            dataset=cursor.fetchone()
            cursor.close()
            return dataset
        except:
            traceback.print_exc()
        return None
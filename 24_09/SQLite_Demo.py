import sqlite3
import pandas as pd
try:
    sqliteConnection=sqlite3.connect('../databases/Chinook_Sqlite.sqlite')
    cursor=sqliteConnection.cursor()
    print('DB Init')
    query='SELECT * FROM InvoiceLine LIMIT 5;'
    cursor.execute(query)
    df=pd.DataFrame(cursor.fetchall())
    print(df)
    cursor.close()
except sqlite3.Error as error:
    print('Error occured -', error)
finally:
    if sqliteConnection:
        sqliteConnection.close()
        print('SQLite Connection closed')
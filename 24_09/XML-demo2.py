import pandas_read_xml as pdx
df = pdx.read_xml("../dataset/SalesTransactions/SalesTransactions.xml", ['UelSample', 'SalesItem'])
print(df)
print(df.iloc[0])
data=df.iloc[0]
print(data[0])
print(data[1])
print(data[1]["OrderID"])
import pandas as pd

# df = pd.read_csv(r"D:\ML\ML_K23416\dataset\SalesTransactions\SalesTransactions.txt", encoding='utf-8', dtype = 'unicode',
#                  sep='\t')
df = pd.read_csv("../dataset/SalesTransactions/SalesTransactions.txt", encoding='utf-8', dtype = 'unicode',
                 sep='\t')
print(df)
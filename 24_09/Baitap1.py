import sqlite3
import pandas as pd

# Đường dẫn tới database
db_path = "../databases/Chinook_Sqlite.sqlite"

# Kết nối và truy vấn
with sqlite3.connect(db_path) as conn:
    query = """
        SELECT c.CustomerId, c.FirstName, c.LastName, c.Country,
               SUM(i.Total) AS TotalSpent
        FROM Customer c
        JOIN Invoice i ON c.CustomerId = i.CustomerId
        GROUP BY c.CustomerId, c.FirstName, c.LastName, c.Country
        ORDER BY TotalSpent DESC
        LIMIT 10;
    """
    top_customers = pd.read_sql_query(query, conn)

# In kết quả
print("Top 10 khách hàng chi tiêu nhiều nhất:")
print(top_customers)

# đầu vào là df, đầu ra là top 3 sản phẩm bán ra có giá trị bán ra cao nhất
import pandas as pd

# Đọc dữ liệu
df = pd.read_csv("../dataset/SalesTransactions/SalesTransactions.csv")

# Tạo cột giá trị bán ra cho từng dòng
df['Total'] = df['UnitPrice'] * df['Quantity'] * (1 - df['Discount'])

# Tính tổng giá trị theo ProductID
product_sales = df.groupby('ProductID')['Total'].sum()

# Sắp xếp giảm dần và lấy top 3
top3_products = product_sales.sort_values(ascending=False).head(3)

# In kết quả
print("Top 3 sản phẩm có giá trị bán ra cao nhất:")
for product_id, total in top3_products.items():
    print(f" - ProductID {product_id}: {total:.2f}")


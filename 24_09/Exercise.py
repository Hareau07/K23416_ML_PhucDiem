import pandas as pd
def find_orders_within_range(df,minValue, maxValue):
    order_totals=df.groupby('OrderID').apply(lambda x: (x['UnitPrice']*x['Quantity']*(1-x['Discount'])).sum())
    orders_within_range = order_totals[(order_totals>= minValue) & (order_totals<=maxValue)]
    unique_orders =df[df['OrderID'].isin(orders_within_range.index)]['OrderID'].drop_duplicates().tolist()
    return unique_orders
df = pd.read_csv('../dataset/SalesTransactions/SalesTransactions.csv')
minValue=float(input("Nhập giá trị min: "))
maxValue = float(input("Nhập gi trị max: "))
result = find_orders_within_range(df,minValue,maxValue)
print('Danh sách các hóa đơn trong phạm vi giá trị từ', minValue, "đến", maxValue, "là:", result)


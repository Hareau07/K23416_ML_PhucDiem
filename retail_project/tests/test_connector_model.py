import traceback

import mysql.connector

from retail_project.models.customer import Customer

server="localhost"
port=3306
database="k23416_retail"
username="root"
password="thoalalen792005"
try:
    conn = mysql.connector.connect(
                    host=server,
                    port=port,
                    database=database,
                    user=username,
                    password=password)
except:
    traceback.print_exc()

#Câu 1: Đăng nhập cho Customer
def login_customer(email, pwd):
    cursor = conn.cursor()
    sql = "SELECT * FROM customer " \
          "WHERE Email = '" + email + "' and Password = '" + pwd + "'"
    cust = None
    cursor.execute(sql)
    dataset = cursor.fetchone()
    if dataset != None:
        cust = Customer(dataset[0], dataset[1], dataset[2], dataset[3], dataset[4], dataset[5])
        # print("ID===", dataset[0])
        # cust.ID, cust.Name, cust.Phone, cust.Email, cust.Password, cust.IsDeleted = dataset
    cursor.close()
    return cust
cust = login_customer("hocamdao@gmail.com", "123")
if cust == None:
    print("Login failed")
else:
    print("Login succeeded")
    print(cust)
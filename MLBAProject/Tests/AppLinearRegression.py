from MLBAProject.Connectors.Connector import Connector
from MLBAProject.Models.PurchaseLinearRegression import PurchaseLinearRegression

connector=Connector(server="localhost", port=3306, database="lecturer_retails", username="Phuc_Diem", password="Huyndiem07@")
connector.connect()
pm=PurchaseLinearRegression(connector=connector)
pm.processTrain(["gender","age"],"price",0.2,0)
#pm.processTrain(["gender","age","payment_method"],"price")
#pm.visualizeActualAndPredictResult()

eresult=pm.evaluate()
print(eresult)

gender="Male"
age=61
pred=pm.predictPriceFromGenderAndAge(gender,age)
print("Gender=%s and Age=%s=>Price=%s"%(gender,age,pred))

gender="Female"
age=61
pred=pm.predictPriceFromGenderAndAge(gender,age)
print("Gender=%s and Age=%s=>Price=%s"%(gender,age,pred))

print("------------------"*10)
pm=PurchaseLinearRegression()
pm.connector=connector
pm.processTrain(["gender","age","payment_method"],"price",0.2,0)

eresult=pm.evaluate()
print(eresult)

gender="Male"
age=61
payment="Credit Card"
pred=pm.predictPriceFromGenderAndAgeAndPayment(gender,age,payment)
print("Gender=%s and Age=%s and payment=%s=>Price=%s"%(gender,age,payment,pred))

gender="Male"
age=61
payment="Debit Card"
pred=pm.predictPriceFromGenderAndAgeAndPayment(gender,age,payment)
print("Gender=%s and Age=%s and payment=%s=>Price=%s"%(gender,age,payment,pred))

gender="Male"
age=61
payment="Cash"
pred=pm.predictPriceFromGenderAndAgeAndPayment(gender,age,payment)
print("Gender=%s and Age=%s and payment=%s=>Price=%s"%(gender,age,payment,pred))

ret=pm.saveModel("../Assets/LR_mymodel.zip")
print("ret save model=%s"%ret)
from MLBAProject.Models.PurchaseLinearRegression import PurchaseLinearRegression

pm=PurchaseLinearRegression()
pm.loadModel("../Assets/LR_mymodel.zip")

gender="Female"
age=61
payment="Cash"
pred=pm.predictPriceFromGenderAndAgeAndPayment(gender,age,payment)
print("Gender=%s and Age=%s and payment=%s=>Price=%s"%(gender,age,payment,pred))
#Gender=Female and Age=61 and payment=Cash=>Price=[692.98688316]
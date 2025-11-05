from MLBAProject.Connectors.Connector import Connector
from MLBAProject.Models.PurchaseStatistic import PurchaseStatistic

connector=Connector(server="localhost", port=3306, database="lecturer_retails", username="Phuc_Diem", password="Huyndiem07@")
connector.connect()
pm=PurchaseStatistic()
pm.connector=connector
pm.execPurchaseHistory()
dfGender=pm.processGenderDistribution()
print(dfGender)
pm.visualizePieChart(dfGender,"gender","count","Gender Distribution")

dfAge=pm.processAgeDistribution(30,50)
print(dfAge)
pm.visualizePlotChart(dfAge,"age","count","Age Distribution 30~50")

dfCategory=pm.processCategoryDistribution()
print(dfCategory)
pm.visualizePieChart(dfCategory,"category","count","Categories Distribution",legend=False)

dfCateSpending=pm.processCategorySpending()
print(dfCateSpending)
pm.visualizeBarChart(dfCateSpending,"category","price","Distribution category and Spending")


dfGenderCategory=pm.processGenderAndCategoryCounter()
print(dfGenderCategory)
pm.visualizeCountPlot(pm.df,"category","count","gender","Distribution gender and category")


dfPayment=pm.processPaymentMethod()
print(dfPayment)
pm.visualizePieChart(dfPayment,"payment_method","count","Payment Distribution",legend=False)

dfShoppingMall=pm.processShoppingMall()
print(dfShoppingMall)
pm.visualizePieChart(dfShoppingMall,"shopping_mall","count","Shopping Mall Distribution",legend=False)

dfGenderCateSpending=pm.processGenderCategorySpending()
print(dfGenderCateSpending)
pm.visualizeBarPlot(dfGenderCateSpending,"category","price","gender","Male and Female category Total Price Spend")

dfAgeGender=pm.processAgeOrderFrequence()
print(dfAgeGender)
pm.visualizeScatterPlot(dfAgeGender,"age","count","Age VS Order Frequence")

dfMonthlySalesAmount=pm.processMonthlySalesAmount()
print(dfMonthlySalesAmount)
pm.visualizeLinePlotChart(dfMonthlySalesAmount,"month","sales_amount","Monthly Variation in Sales Amount")

dfMonthlyAndYearSalesAmount=pm.processMonthlyAndYearSalesAmount()
print(dfMonthlyAndYearSalesAmount)
pm.visualizeLinePlotChart(dfMonthlyAndYearSalesAmount,"month","sales_amount","Monthly Variation in Sales Amount Over Years",hue="year")
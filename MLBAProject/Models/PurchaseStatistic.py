from matplotlib import pyplot as plt
import seaborn as sns
import pandas as pd
class PurchaseStatistic:
    def __init__(self,connector=None):
        self.connector = connector
        self.lasted_df=None
    def execPurchaseHistory(self,tableName=None):
        if tableName==None:
            sql="select * from purchasehistory"
        else:
            sql = "select * from %s"%tableName
        self.df=self.connector.queryDataset(sql)
        self.lasted_df=self.df
        return self.df
    def printHead(self,row):
        print(self.df.head(row))
    def printTail(self,row):
        print(self.df.tail(row))
    def printInfo(self):
        print(self.df.info())
    def printDecsribe(self):
        print(self.df.describe())
    def dateProcessing(self):
        self.df['invoice_date'] = pd.to_datetime(self.df['invoice_date'] , format = '%d/%m/%Y')
        self.df['month'] = self.df['invoice_date'].dt.month
        self.df['year'] = self.df['invoice_date'].dt.year
        self.lasted_df = self.df
    def processGenderDistribution(self):
        self.dfGender = self.df.gender.value_counts().reset_index()
        self.lasted_df = self.dfGender
        return self.dfGender
    def processAgeDistribution(self):
        self.dfAges = self.df.age.value_counts().reset_index()
        self.dfAges.sort_values(by=['age'], ascending=True, inplace=True)
        self.lasted_df = self.dfAges
        return self.dfAges
    def processAgeDistribution(self,fromAge,toAge):
        self.dfAges = self.df[(self.df.age >= fromAge) & (self.df.age <= toAge)].age.value_counts().reset_index()
        self.dfAges.sort_values(by=['age'], ascending=True,inplace=True)
        self.lasted_df = self.dfAges
        return self.dfAges
    def visualizePieChart(self,df,columnLabel,columnStatistic,title,legend=True):
        explode=[0.1]
        for i in range(len(df[columnLabel])-1):
            explode.append(0)
        plt.figure(figsize=(8, 6))
        plt.pie(df[columnStatistic], labels=df[columnLabel], autopct='%1.2f%%',explode=explode)
        if legend:
            plt.legend(df[columnLabel])
        plt.title(title)
        plt.show()
    def visualizePlotChart(self,df,columnX,columnY,title):
        plt.figure(figsize=(8, 6))
        plt.plot(df[columnX], df[columnY])
        plt.legend([columnX,columnY])
        plt.title(title)
        plt.xlabel(columnX)
        plt.ylabel(columnY)
        plt.grid()
        plt.show()
    def processCategoryDistribution(self):
        self.dfCategory = self.df.category.value_counts().reset_index()
        self.lasted_df = self.dfCategory
        return self.dfCategory
    def processGenderAndCategoryCounter(self):
        self.df_gender_order = self.df[['gender', 'category']]\
                                   .groupby(['gender', 'category'])\
                                   .value_counts()\
                                   .reset_index(name="count")
        self.lasted_df = self.df_gender_order
        return self.df_gender_order
    def processCategorySpending(self):
        self.df_cate_spending = self.df.groupby(['category'])["price"].sum().reset_index(name="price")
        self.lasted_df = self.df_cate_spending
        return self.df_cate_spending
    def processGenderCategorySpending(self):
        self.df_gender_cate_spending = self.df.groupby(['gender','category'])["price"].sum().reset_index(name="price")
        self.lasted_df = self.df_gender_cate_spending
        return self.df_gender_cate_spending
    def visualizeCountPlot(self,df,columnX,columnY,hueColumn,title):
        plt.figure(figsize=(8, 6))
        ax=sns.countplot(x=columnX,hue=hueColumn,data=df)
        plt.title(title)
        plt.xlabel(columnX)
        plt.ylabel(columnY)
        plt.grid()
        plt.legend()
        plt.show()
    def visualizeBarPlot(self,df,columnX,columnY,hueColumn,title,alpha=0.8,width=0.6):
        plt.figure(figsize=(8, 6))
        plt.ticklabel_format(useOffset=False, style='plain')
        ax=sns.barplot(data=df,x=columnX,y=columnY,hue=hueColumn,alpha=alpha,width=width)
        plt.title(title)
        plt.xlabel(columnX)
        plt.ylabel(columnY)
        plt.grid()
        plt.legend()
        plt.show()
    def visualizeBarChart(self,df,columnX,columnY,title):
        plt.figure(figsize=(8, 6))
        plt.ticklabel_format(useOffset=False, style='plain')
        plt.bar(df[columnX],df[columnY])
        plt.title(title)
        plt.xlabel(columnX)
        plt.ylabel(columnY)
        plt.grid()
        plt.show()
    def visualizeScatterPlot(self,df,columnX,columnY,title):
        plt.figure(figsize=(8, 6))
        plt.ticklabel_format(useOffset=False, style='plain')
        sns.scatterplot(data=df,x= columnX,y=columnY)
        plt.title(title)
        plt.xlabel(columnX)
        plt.ylabel(columnY)
        plt.grid()
        plt.show()
    def processPaymentMethod(self):
        self.payment = self.df['payment_method'].value_counts().reset_index(name="count").rename(columns={"index": "payment_method"})
        self.lasted_df = self.payment
        return self.payment
    def processShoppingMall(self):
        self.dfShoppingMall = self.df['shopping_mall'].value_counts().reset_index(name="count").rename(columns={"index": "shopping_mall"})
        self.lasted_df = self.dfShoppingMall
        return self.dfShoppingMall
    def processAgeOrderFrequence(self):
        #self.dfAgeGender = self.df.groupby(['age', 'gender'])['age'].value_counts().reset_index(name="count")
        self.dfAgeGender = self.df[['age', 'gender']].groupby(['age', 'gender']).value_counts().reset_index(name="count")
        self.lasted_df = self.dfAgeGender
        return self.dfAgeGender
    def processAgeSalesAmount(self):
        self.dfSalesAmount = self.df.copy(deep=True)
        self.dfSalesAmount['sales_amount'] = self.dfSalesAmount['quantity'] * self.dfSalesAmount['price']
        self.lasted_df = self.dfSalesAmount
        return self.dfSalesAmount
    def processMonthlySalesAmount(self):
        self.dfMonthlySalesAmount=self.df.copy(deep=True)
        self.dfMonthlySalesAmount['sales_amount'] = self.dfMonthlySalesAmount['quantity'] * self.dfMonthlySalesAmount['price']

        self.dfMonthlySalesAmount['invoice_date'] = pd.to_datetime(self.dfMonthlySalesAmount['invoice_date'],
                                            format='%d/%m/%Y')  # convert invoice date to date time format

        self.dfMonthlySalesAmount['month'] = self.dfMonthlySalesAmount['invoice_date'].dt.month

        self.dfMonthlySalesAmount = self.dfMonthlySalesAmount.groupby('month')['sales_amount'].sum().reset_index()
        self.lasted_df = self.dfMonthlySalesAmount
        return self.dfMonthlySalesAmount
    def visualizeLinePlotChart(self,df,columnX,columnY,tile,hue=None):
        plt.figure(figsize=(8, 6))
        plt.ticklabel_format(useOffset=False,style="plain")
        sns.lineplot(data=df,x=columnX, y=columnY, marker='o', color='orange',hue=hue)
        plt.xlabel(columnX)
        plt.ylabel(columnY)
        plt.title(tile)
        plt.legend(loc='upper right')
        plt.xticks(range(1, 13), ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
        plt.show()
    def processMonthlyAndYearSalesAmount(self):
        self.dfMonthlyAndYearSalesAmount = self.df.copy(deep=True)
        self.dfMonthlyAndYearSalesAmount['invoice_date'] = pd.to_datetime(self.dfMonthlyAndYearSalesAmount['invoice_date'],
                                                                   format='%d/%m/%Y')
        self.dfMonthlyAndYearSalesAmount['month'] = self.dfMonthlyAndYearSalesAmount['invoice_date'].dt.month
        self.dfMonthlyAndYearSalesAmount['year'] = self.dfMonthlyAndYearSalesAmount['invoice_date'].dt.year
        self.dfMonthlyAndYearSalesAmount['sales_amount'] = self.dfMonthlyAndYearSalesAmount['quantity'] * self.dfMonthlyAndYearSalesAmount['price']
        self.dfMonthlyAndYearSalesAmount = self.dfMonthlyAndYearSalesAmount.groupby(['year', 'month'], as_index=False).agg({'sales_amount': 'sum'})
        self.lasted_df = self.dfMonthlyAndYearSalesAmount
        return self.dfMonthlyAndYearSalesAmount
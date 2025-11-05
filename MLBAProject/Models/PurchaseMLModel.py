# Features encoding
from sklearn.preprocessing import LabelEncoder
import seaborn as sns
from matplotlib import pyplot as plt

from MLBAProject.Models.PurchaseStatistic import PurchaseStatistic


class PurchaseMLModel(PurchaseStatistic):
    def __init__(self, connector=None):
        super().__init__(connector)
        self.le = LabelEncoder()

    def processTransformByColumns(self, df, columns):
        for col in columns:
            x = df[col]
            df[col] = self.le.fit_transform(x)

    def processTransform(self):
        categorical_feature = ['gender', 'category', 'payment_method', 'shopping_mall']
        numerical_feature = ['age', 'quantity', 'month', 'year']
        dropping = ['customer_id', 'invoice_no', 'day', 'invoice_date']
        result = ['price']
        self.dfTransform = self.df.copy(deep=True)
        self.dfTransform[["day", "month", "year"]] = self.dfTransform["invoice_date"].str.split("/", expand=True)
        self.dfTransform.drop(dropping, axis=1, inplace=True)
        for col in categorical_feature:
            x = self.dfTransform[col]
            self.dfTransform[col] = self.le.fit_transform(x)
        return self.dfTransform

    def buildCorrelationMatrix(self, df):
        plt.figure(figsize=(8, 6))
        df_corr = df.corr(numeric_only=True)  # Generate correlation matrix
        ax = sns.heatmap(df_corr, annot=True)
        plt.show()

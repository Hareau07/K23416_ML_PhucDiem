import random
from random import random
import plotly.graph_objects as go

from PyQt6 import QtGui, QtCore
from PyQt6.QtCore import Qt, QSize
from PyQt6.QtGui import QAction, QIcon, QPixmap
from PyQt6.QtWidgets import QMessageBox, QTableWidgetItem, QMainWindow, QDialog, QComboBox, QPushButton, QCheckBox, \
    QListWidgetItem, QFileDialog
from matplotlib import pyplot as plt
import seaborn as sns
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qt5agg import NavigationToolbar2QT as NavigationToolbar

from MLBAProject.Connectors.Connector import Connector
from MLBAProject.Models.PurchaseLinearRegression import PurchaseLinearRegression
from MLBAProject.Models.PurchaseStatistic import PurchaseStatistic
from MLBAProject.UI.ChartHandle import ChartHandle
from MLBAProject.UI.DatabaseConnectEx import DatabaseConnectEx
from MLBAProject.UI.MainWindow import Ui_MainWindow
import traceback


import matplotlib

from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg
from matplotlib.figure import Figure


from matplotlib.backends.backend_qt5agg import NavigationToolbar2QT as NavigationToolbar
import random


class MainWindowEx(Ui_MainWindow):
    def __init__(self):
        self.purchaseLinearRegression = PurchaseLinearRegression()
        self.databaseConnectEx=DatabaseConnectEx()
        self.databaseConnectEx.parent=self
        self.chartHandle= ChartHandle()
    def setupUi(self, MainWindow):
        super().setupUi(MainWindow)
        self.MainWindow=MainWindow
        self.verticalLayoutFunctions.setAlignment(Qt.AlignmentFlag.AlignTop)
        self.setupPlot()

        self.actionConnection.triggered.connect(self.openDatabaseConnectUI)

        self.pushButtonPurchaseRatesByGender.clicked.connect(self.showPurchaseRatesByGender)
        self.pushButtonSalesFlucuationsByYearAndMonth.clicked.connect(self.showSalesFlucuationsByYearAndMonth)
        self.pushButtonPurchaseCountingByCategory.clicked.connect(self.showPurchaseCountingByCategory)
        self.pushButtonPurchaseRatesByAgeGroup.clicked.connect(self.showPurchaseRatesByAgeGroup)
        self.pushButtonPurchaseCountingByCategory.clicked.connect(self.showPurchaseCountingByCategory)
        self.pushButtonPurchaseValueByCategory.clicked.connect(self.showPurchaseValueByCategory)
        self.pushButtonPurchaseByCategoryAndGender.clicked.connect(self.showPurchaseByCategoryAndGender)
        self.pushButtonPaymentMethod.clicked.connect(self.showPaymentMethod)
        self.pushButtonPurchaseRatesByShoppingMall.clicked.connect(self.showPurchaseRatesByShoppingMall)
        self.pushButtonProductSpendingByGender.clicked.connect(self.showProductSpendingByGender)
        self.pushButtonPurchaseFrequenceByAge.clicked.connect(self.showShowPurchaseFrequenceByAge)
        self.pushButtonSalesFluctuationsByMonth.clicked.connect(self.showpushButtonSalesFluctuationsByMonth)
        self.checkEnableWidget(False)

        self.pushButtonTrainModel.clicked.connect(self.processTrainModel)
        self.pushButtonEvaluate.clicked.connect(self.processEvaluateTrainedModel)
        self.pushButtonSavePath.clicked.connect(self.processPickSavePath)
        self.pushButtonSaveModel.clicked.connect(self.processSaveTrainedModel)
        self.pushButtonLoadModel.clicked.connect(self.processLoadTrainedModel)
        self.pushButtonPredict.clicked.connect(self.processPrediction)
    def show(self):
        self.MainWindow.show()
    def checkEnableWidget(self,flag=True):
        self.pushButtonPurchaseRatesByGender.setEnabled(flag)
        self.pushButtonPurchaseRatesByAgeGroup.setEnabled(flag)
        self.pushButtonPurchaseCountingByCategory.setEnabled(flag)
        self.pushButtonPurchaseValueByCategory.setEnabled(flag)
        self.pushButtonPurchaseByCategoryAndGender.setEnabled(flag)
        self.pushButtonPaymentMethod.setEnabled(flag)
        self.pushButtonPurchaseRatesByShoppingMall.setEnabled(flag)

        self.pushButtonProductSpendingByGender.setEnabled(flag)
        self.pushButtonPurchaseFrequenceByAge.setEnabled(flag)
        self.pushButtonSalesFluctuationsByMonth.setEnabled(flag)
        self.pushButtonSalesFlucuationsByYearAndMonth.setEnabled(flag)

    def setupPlot(self):
        self.figure = plt.figure()
        self.canvas = FigureCanvas(self.figure)
        self.toolbar = NavigationToolbar(self.canvas, self.MainWindow)

        # adding tool bar to the layout
        self.verticalLayoutPlot.addWidget(self.toolbar)
        # adding canvas to the layout
        self.verticalLayoutPlot.addWidget(self.canvas)
    def openDatabaseConnectUI(self):
        dbwindow = QMainWindow()
        self.databaseConnectEx.setupUi(dbwindow)
        self.databaseConnectEx.show()
    def showDataIntoTableWidget(self,df):
        self.tableWidgetStatistic.setRowCount(0)
        self.tableWidgetStatistic.setColumnCount(len(df.columns))
        for i in range(len(df.columns)):
            columnHeader = df.columns[i]
            self.tableWidgetStatistic.setHorizontalHeaderItem(i, QTableWidgetItem(columnHeader))
        row = 0
        for item in df.iloc:
            arr = item.values.tolist()
            self.tableWidgetStatistic.insertRow(row)
            j=0
            for data in arr:
                self.tableWidgetStatistic.setItem(row, j, QTableWidgetItem(str(data)))
                j=j+1
            row = row + 1

    def showPurchaseCountingByCategory(self):
        self.purchaseLinearRegression.connector = self.databaseConnectEx.connector
        self.purchaseLinearRegression.execPurchaseHistory()
        self.purchaseLinearRegression.processCategoryDistribution()
        print(self.purchaseLinearRegression.dfCategory)

        df = self.purchaseLinearRegression.dfCategory

        self.showDataIntoTableWidget(df)

        columnLabel = "category"
        columnStatistic = "count"
        title = "Categories Distribution"
        legend = False
        #self.visualizePieChart(df, columnLabel, columnStatistic, title, legend)
        self.chartHandle.visualizePieChart(self.figure,self.canvas,df, columnLabel, columnStatistic, title, legend)

    def showPurchaseRatesByGender(self):
        self.purchaseLinearRegression.connector = self.databaseConnectEx.connector
        self.purchaseLinearRegression.execPurchaseHistory()
        self.purchaseLinearRegression.processGenderDistribution()
        print(self.purchaseLinearRegression.dfGender)

        df = self.purchaseLinearRegression.dfGender

        self.showDataIntoTableWidget(df)

        columnLabel = "gender"
        columnStatistic = "count"
        title = "Gender Distribution"
        legend = True
        self.chartHandle.visualizePieChart(self.figure,self.canvas,df, columnLabel, columnStatistic, title, legend)
        #self.visualizePieChart(df, columnLabel, columnStatistic, title, legend)

    def showSalesFlucuationsByYearAndMonth(self):
        self.purchaseLinearRegression.connector = self.databaseConnectEx.connector
        self.purchaseLinearRegression.execPurchaseHistory()
        self.purchaseLinearRegression.processMonthlyAndYearSalesAmount()
        print(self.purchaseLinearRegression.dfMonthlyAndYearSalesAmount)
        df = self.purchaseLinearRegression.dfMonthlyAndYearSalesAmount
        self.showDataIntoTableWidget(df)
        self.chartHandle.visualizeLinePlotChart(self.figure,self.canvas, self.purchaseLinearRegression.dfMonthlyAndYearSalesAmount, "month", "sales_amount",
                                    "Monthly Variation in Sales Amount Over Years", hue="year", xticks=True)
        #self.visualizeLinePlotChart(self.purchaseLinearRegression.dfMonthlyAndYearSalesAmount, "month", "sales_amount",
        #                            "Monthly Variation in Sales Amount Over Years", hue="year", xticks=True)
    def showPurchaseRatesByAgeGroup(self):
        self.purchaseLinearRegression.connector = self.databaseConnectEx.connector
        self.purchaseLinearRegression.execPurchaseHistory()
        fromAge=int(self.lineEditFromAge.text())
        toAge=int(self.lineEditToAge.text())
        self.purchaseLinearRegression.processAgeDistribution(fromAge,toAge)
        print(self.purchaseLinearRegression.dfAges)

        df = self.purchaseLinearRegression.dfAges

        self.showDataIntoTableWidget(df)
        columnLabel= "age"
        columnStatistic ="count"
        title= "Age Distribution %s~%s"%(fromAge,toAge)
        hue = None
        self.chartHandle.visualizeLinePlotChart(self.figure,self.canvas,df, columnLabel, columnStatistic,title, hue)
        #self.visualizeLinePlotChart(df, columnLabel, columnStatistic,title, hue)
    def showPurchaseCountingByCategory(self):
        self.purchaseLinearRegression.connector = self.databaseConnectEx.connector
        self.purchaseLinearRegression.execPurchaseHistory()
        df = self.purchaseLinearRegression.processCategoryDistribution()
        self.showDataIntoTableWidget(df)
        columnLabel = "category"
        columnStatistic = "count"
        title = "Categories Distribution"
        legend = False
        hue=None
        self.chartHandle.visualizeLinePlotChart(self.figure,self.canvas,df, columnLabel, columnStatistic, title, hue)
        #self.visualizePieChart(df, columnLabel, columnStatistic, title, legend)
    def showPurchaseValueByCategory(self):
        # self.purchaseLinearRegression.connector = self.databaseConnectEx.connector
        # self.purchaseLinearRegression.execPurchaseHistory()
        df = self.purchaseLinearRegression.processCategorySpending()
        self.showDataIntoTableWidget(df)
        columnLabel = "category"
        columnStatistic = "price"
        title = "Distribution category and Spending"
        self.chartHandle.visualizeBarChart(self.figure,self.canvas,df,columnLabel,columnStatistic,title)
        #self.visualizeBarChart(df,columnLabel,columnStatistic,title)
    def showPurchaseByCategoryAndGender(self):
        # self.purchaseLinearRegression.connector = self.databaseConnectEx.connector
        # self.purchaseLinearRegression.execPurchaseHistory()
        df = self.purchaseLinearRegression.processGenderAndCategoryCounter()
        self.showDataIntoTableWidget(df)
        df=self.purchaseLinearRegression.df
        columnLabel = "category"
        columnStatistic = "count"
        hue="gender"
        title = "Distribution gender and category"
        self.chartHandle.visualizeMultiBarChart(self.figure,self.canvas,df, columnLabel, columnStatistic,hue, title)
        #self.visualizeMultiBarChart(df, columnLabel, columnStatistic,hue, title)
    def showPaymentMethod(self):
        # self.purchaseLinearRegression.connector = self.databaseConnectEx.connector
        # self.purchaseLinearRegression.execPurchaseHistory()
        df = self.purchaseLinearRegression.processPaymentMethod()
        self.showDataIntoTableWidget(df)
        columnLabel = "payment_method"
        columnStatistic = "count"
        title = "Payment Distribution"
        legend = False
        self.chartHandle.visualizePieChart(self.figure,self.canvas,df, columnLabel, columnStatistic, title, legend)
        #self.visualizePieChart(df, columnLabel, columnStatistic, title, legend)
    def showPurchaseRatesByShoppingMall(self):
        # self.purchaseLinearRegression.connector = self.databaseConnectEx.connector
        # self.purchaseLinearRegression.execPurchaseHistory()
        df = self.purchaseLinearRegression.processShoppingMall()
        self.showDataIntoTableWidget(df)
        columnLabel = "shopping_mall"
        columnStatistic = "count"
        title = "Shopping Mall Distribution"
        legend = False
        self.chartHandle.visualizePieChart(self.figure,self.canvas,df, columnLabel, columnStatistic, title, legend)
        #self.visualizePieChart(df, columnLabel, columnStatistic, title, legend)
    def showProductSpendingByGender(self):
        # self.purchaseLinearRegression.connector = self.databaseConnectEx.connector
        # self.purchaseLinearRegression.execPurchaseHistory()
        df = self.purchaseLinearRegression.processGenderCategorySpending()
        self.showDataIntoTableWidget(df)
        columnLabel = "category"
        columnStatistic = "price"
        hue="gender"
        title = "Male and Female category Total Price Spend"
        legend = False
        self.chartHandle.visualizeBarPlot(self.figure,self.canvas,df, columnLabel, columnStatistic,hue, title)
        #self.visualizeBarPlot(df, columnLabel, columnStatistic,hue, title)

    def showShowPurchaseFrequenceByAge(self):
        # self.purchaseLinearRegression.connector = self.databaseConnectEx.connector
        # self.purchaseLinearRegression.execPurchaseHistory()
        df = self.purchaseLinearRegression.processAgeOrderFrequence()
        self.showDataIntoTableWidget(df)
        columnLabel = "age"
        columnStatistic = "count"
        title = "Age VS Order Frequence"
        self.chartHandle.visualizeScatterPlot(self.figure,self.canvas,df, columnLabel,columnStatistic, title)
        #self.visualizeScatterPlot(df, columnLabel,columnStatistic, title)

    def showpushButtonSalesFluctuationsByMonth(self):
        # self.purchaseLinearRegression.connector = self.databaseConnectEx.connector
        # self.purchaseLinearRegression.execPurchaseHistory()

        df=self.purchaseLinearRegression.processMonthlySalesAmount()
        print(df)

        self.showDataIntoTableWidget(df)
        columnLabel = "month"
        columnStatistic = "sales_amount"
        title = "Monthly Variation in Sales Amount"
        hue = None
        self.chartHandle.visualizeLinePlotChart(self.figure,self.canvas,df, columnLabel, columnStatistic, title, hue)
    def processTrainModel(self):
        columns_input=["gender","age"]
        column_target="price"
        if self.radioButtonGenderAgePayment.isChecked():
            columns_input=["gender","age","payment_method"]
        test_size=float(self.lineEditTestSize.text())/100
        random_state=int(self.lineEditRandomState.text())
        self.purchaseLinearRegression = PurchaseLinearRegression()
        self.purchaseLinearRegression.connector = self.databaseConnectEx.connector
        self.purchaseLinearRegression.processTrain(
            columns_input,
            column_target,
            test_size,
            random_state)
        dlg = QMessageBox(self.MainWindow)
        dlg.setWindowTitle("Info")
        dlg.setIcon(QMessageBox.Icon.Information)
        dlg.setText("Train machine learning model successful!")
        buttons = QMessageBox.StandardButton.Yes
        dlg.setStandardButtons(buttons)
        button = dlg.exec()
    def processEvaluateTrainedModel(self):
        result = self.purchaseLinearRegression.evaluate()
        self.lineEditMAE.setText(str(result.MAE))
        self.lineEditMSE.setText(str(result.MSE))
        self.lineEditRMSE.setText(str(result.RMSE))
        self.lineEditR2SCore.setText(str(result.R2_SCORE))
    def processPickSavePath(self):
        filters = "trained model file (*.zip);;All files(*)"
        filename, selected_filter = QFileDialog.getSaveFileName(
            self.MainWindow,
            filter=filters,
        )
        self.lineEditPath.setText(filename)
    def processSaveTrainedModel(self):
        trainedModelPath=self.lineEditPath.text()
        if trainedModelPath=="":
            return
        ret = self.purchaseLinearRegression.saveModel(trainedModelPath)
        dlg = QMessageBox(self.MainWindow)
        dlg.setWindowTitle("Info")
        dlg.setIcon(QMessageBox.Icon.Information)
        dlg.setText(f"Saved Trained machine learning model successful at [{trainedModelPath}]!")
        buttons = QMessageBox.StandardButton.Yes
        dlg.setStandardButtons(buttons)
        button = dlg.exec()
    def processLoadTrainedModel(self):
        # setup for QFileDialog
        filters = "trained model file (*.zip);;All files(*)"
        filename, selected_filter = QFileDialog.getOpenFileName(
            self.MainWindow,
            filter=filters,
        )
        if filename=="":
            return
        self.lineEditLocationLoadTrainedModel.setText(filename)
        self.purchaseLinearRegression.loadModel(filename)
        dlg = QMessageBox(self.MainWindow)
        dlg.setWindowTitle("Info")
        dlg.setIcon(QMessageBox.Icon.Information)
        dlg.setText(f"Load Trained machine learning model successful from [{filename}]!")
        buttons = QMessageBox.StandardButton.Yes
        dlg.setStandardButtons(buttons)
        button = dlg.exec()
    def processPrediction(self):
        gender = self.lineEditGender.text()
        age = int(self.lineEditAge.text())
        payment = self.lineEditPaymentMethod.text()
        if len(self.purchaseLinearRegression.trainedmodel.columns_input)==3:
            predicted_price = self.purchaseLinearRegression.predictPriceFromGenderAndAgeAndPayment(gender, age, payment)
        else:
            predicted_price = self.purchaseLinearRegression.predictPriceFromGenderAndAge(gender, age)
        self.lineEditPredictedPrice.setText(str(predicted_price[0]))
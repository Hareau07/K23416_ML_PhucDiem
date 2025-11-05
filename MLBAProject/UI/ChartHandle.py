import seaborn as sns
class ChartHandle:
    def getExplode(self,df,columnLabel):
        explode = [0.1]
        for i in range(len(df[columnLabel]) - 1):
            explode.append(0)
        return explode
    def visualizePieChart(self,figure,canvas,df,columnLabel,columnStatistic,title,legend=True):
        explode=self.getExplode(df,columnLabel)
        figure.clear()
        ax = figure.add_subplot(111)
        ax.pie(df[columnStatistic], labels=df[columnLabel], autopct='%1.2f%%', explode=explode)
        if legend:
            ax.legend(df[columnLabel],loc ='lower right')
        ax.set_title(title)
        canvas.draw()
    def visualizeLinePlotChart(self,figure,canvas,df,columnX,columnY,tile,hue=None,xticks=False):
        figure.clear()
        ax = figure.add_subplot(111)
        ax.ticklabel_format(useOffset=False,style="plain")
        ax.grid()
        sns.lineplot(data=df,x=columnX, y=columnY, marker='o', color='orange',hue=hue)
        ax.set_xlabel(columnX)
        ax.set_ylabel(columnY)
        ax.set_title(tile)
        ax.legend(loc ='lower right')
        #ax.set_xticks(range(1, 13), ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
        if xticks==True:
            ax.set_xticks(range(1, 13), ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
        canvas.draw()
    def visualizeBarChart(self,figure,canvas,df,columnX,columnY,title):
        figure.clear()
        ax = figure.add_subplot(111)
        ax.ticklabel_format(useOffset=False, style="plain")
        ax.grid()
        ax.bar(df[columnX],df[columnY])
        ax.set_title(title)
        ax.set_xlabel(columnX)
        ax.set_ylabel(columnY)
        canvas.draw()
    def visualizeBarPlot(self,figure,canvas,df,columnX,columnY,hueColumn,title,alpha=0.8,width=0.6):
        figure.clear()
        ax = figure.add_subplot(111)
        ax.ticklabel_format(useOffset=False, style="plain")
        ax.grid()
        ax=sns.barplot(data=df,x=columnX,y=columnY,hue=hueColumn,alpha=alpha,width=width)
        ax.set_title(title)
        ax.set_xlabel(columnX)
        ax.set_ylabel(columnY)
        canvas.draw()
    def visualizeMultiBarChart(self,figure,canvas,df,columnX,columnY,hueColumn,title):
        figure.clear()
        ax = figure.add_subplot(111)
        ax.ticklabel_format(useOffset=False, style="plain")
        ax.grid()
        sns.countplot(x=columnX, hue=hueColumn, data=df)
        ax.set_title(title)
        ax.set_xlabel(columnX)
        ax.set_ylabel(columnY)
        canvas.draw()
    def visualizeScatterPlot(self,figure,canvas,df,columnX,columnY,title):
        figure.clear()
        ax = figure.add_subplot(111)
        ax.ticklabel_format(useOffset=False, style="plain")
        ax.grid()
        sns.scatterplot(data=df,x= columnX,y=columnY)
        ax.set_title(title)
        ax.set_xlabel(columnX)
        ax.set_ylabel(columnY)
        canvas.draw()
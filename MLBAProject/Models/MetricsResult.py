class MetricsResult:
    def __init__(self,mae,mse,rmse,r2_score):
        self.MAE=mae
        self.MSE=mse
        self.RMSE=rmse
        self.R2_SCORE=r2_score
    def __str__(self):
        result="MAE=%s"%self.MAE+"\n"+"MSE=%s"%self.MSE+"\n"+"RMSE=%s"%self.RMSE+"\n"+"R2_SCORE=%s"%self.R2_SCORE+"\n"
        return result
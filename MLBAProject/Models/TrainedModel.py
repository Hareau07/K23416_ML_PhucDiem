class TrainedModel:
    def __init__(self,model=None,X_train=None,X_test=None,y_train=None,y_test=None,columns_input=None,column_target=None):
        self.model=model
        self.X_train = X_train
        self.X_test = X_test
        self.y_train = y_train
        self.y_test = y_test
        self.columns_input=columns_input
        self.column_target=column_target
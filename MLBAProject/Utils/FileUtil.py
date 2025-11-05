import pickle
import traceback
class FileUtil:
    @staticmethod
    def saveModel(model,filename):
        try:
            pickle.dump(model,open(filename,'wb'))
            return True
        except:
            traceback.print_exc()
            return False
    @staticmethod
    def loadModel(filename):
        try:
            model=pickle.load(open(filename,'rb'))
            return model
        except:
            traceback.print_exc()
            return None
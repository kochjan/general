import numpy as np
from keras.optimizers import SGD
from keras.models import model_from_json
from keras.models import Sequential

class CaptchaRecognize:
    def __init__(self):
        self.model = None
        self.lable = ["2", "3", "4", "6", "7", "8", "9", "A", "C", "D", "E", "F", "G","H", "J",
                      "K", "L", "N", "P", "Q", "R", "T", "U", "V", "X", "Y", "Z"]

    def one_hot_reverse(self,onehot):
        return self.lable[np.where(onehot==1)[0][0]]

    def load_model(self):
        self.model = model_from_json(open("cnn_captcha.json").read())
        self.model.load_weights("TWSE_captcha_weights.h5")

        sgd = SGD(lr=0.01, decay=1e-6, momentum=0.9, nesterov=True)
        self.model.compile(loss="categorical_crossentropy",
                      optimizer=sgd,
                      metrics=["accuracy"])
    def preprocess(self,image):
        X = []
        imgpos = [[5,45],[45,85],[85,125],[125,165],[160,200]]
        imgarray = np.asarray(image)
        for tid in range(5):
            X.append(imgarray[10:50,imgpos[tid][0]:imgpos[tid][1]].reshape(3,40,40))
        X = np.array(X).astype("float32")
        X /= 255
        return X

    def captcha_predict(self,X):
        if type(self.model) != Sequential:
            self.load_model()
        ans = self.model.predict(X)
        captcha = ""
        for i in ans:
            captcha += self.lable[i.argmax()]
        return captcha

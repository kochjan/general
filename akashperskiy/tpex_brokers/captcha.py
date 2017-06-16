from keras.models import model_from_json
import numpy as np

class CaptchaRecognize:
    def __init__(self):
        self.model = None
        self.lable = ['1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D',
                      'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'P', 'Q', 'R',
                      'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']

    def one_hot_reverse(self ,onehot):
        return self.lable[np.where( onehot ==1)[0][0]]

    def load_model(self):
        self.model = model_from_json(open('TPEX_cnn_captcha.json').read())
        self.model.load_weights('TPEX_captcha_weights.h5')

        self.model.compile(loss='categorical_crossentropy',
                           optimizer='sgd',
                           metrics=['accuracy'])

    def preprocess(self ,image):
        X = []
        imgpos = [[0 ,32] ,[25 ,57] ,[49 ,81] ,[72 ,104] ,[98 ,130]]
        imgarray = np.asarray(image.convert(mode='RGB'))
        for tid in range(5):
            X.append(imgarray[: ,imgpos[tid][0]:imgpos[tid][1]].reshape(3 ,32 ,30))
        X = np.array(X).astype('float32')
        X /= 255
        return X

    def captcha_predict(self ,X):
        if not self.model:
            self.load_model()
        ans = self.model.predict(X)
        captcha = ''
        for i in ans:
            captcha += self.lable[i.argmax()]
        return captcha

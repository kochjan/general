#!/usr/bin/env python
# -*- coding: utf-8 -*-
import requests
import io
from PIL import Image
import captcha
from bs4 import BeautifulSoup
import pandas as pd
import os

class TWSE:
    def __init__(self, stock_ids):
        self.rs = requests.session()
        self.base_url = "http://bsr.twse.com.tw/bshtm/"
        self.stock_ids = stock_ids
        self.base_page = "bsMenu.aspx"
        self.csv_page = "bsContent.aspx"
        self.data = []
        self.ocr = captcha.CaptchaRecognize()
        self.ocr.load_model()

    def get_captcha(self, src):
        captcha = self.rs.get(self.base_url + src, stream=True, verify=False)
        return captcha.content

    def get_captcha_src(self):
        req = self.rs.get(self.base_url + self.base_page)
        dom = BeautifulSoup(req.text, "html.parser")
        panel = dom.find("div", {"id":"Panel_bshtm"})
        img_src = panel.find('img')['src']
        self.VIEWSTATE = dom.select('#__VIEWSTATE')[0].attrs['value']
        self.EVENTVALIDATION = dom.select('#__EVENTVALIDATION')[0].attrs['value']
        return img_src

    def payload_once(self, prediction, stock_id):
        payload ={
            '__EVENTTARGET' : '',
            '__EVENTARGUMENT' : '',
            '__LASTFOCUS' : '',
            '__VIEWSTATE' : '%s' % self.VIEWSTATE,
            '__EVENTVALIDATION' : '%s' % self.EVENTVALIDATION,
            'RadioButton_Normal' : 'RadioButton_Normal',
            'TextBox_Stkno' : '%s' % str(stock_id),
            'CaptchaControl1' : '%s' % prediction,
            'btnOK' : '查詢'
            }
        resp = self.rs.post(self.base_url + self.base_page, data = payload)
        resp = self.rs.get(self.base_url + self.csv_page)
        buf = io.StringIO(resp.text)
        for i in range(4):
            buf.readline()
        df = pd.read_csv(buf, encoding = 'utf8')
        return df

    def stock_enumeration(self):
        for stock_id in self.stock_ids:
            self.captcha = Image.open(io.BytesIO(self.get_captcha(self.get_captcha_src())))
            processed_captcha = self.ocr.preprocess(self.captcha)
            prediction = self.ocr.captcha_predict(processed_captcha)
            df = self.payload_once(prediction, stock_id)
            self.data.append(df)
            self.rs = requests.session()
        return self.data

#!/usr/bin/env python
# -*- coding: utf-8 -*-
######################################################################
## Copyright (c) Nipun Capital, L.P., 2011-2017.  All rights reserved.
##
## CONFIDENTIAL
##
## This unpublished material is proprietary to Nipun Capital, L.P.
## The algorithms, methods, techniques and information disclosed or
## described herein are trade secrets and/or confidential information
## owned by Nipun Capital, L.P.  Any use, reproduction or
## distribution, in whole or in part, is prohibited, except by express
## written permission of Nipun Capital, L.P.
######################################################################
import requests
import io
from PIL import Image
import captcha
from bs4 import BeautifulSoup
import pandas as pd
import os
import datetime
import dateutil

class TWSE:
    def __init__(self, stock_ids):
        self.rs = requests.session()
        self.base_url = "http://bsr.twse.com.tw/bshtm/"
        self.stock_ids = stock_ids
        self.base_page = "bsMenu.aspx"
        self.csv_page = "bsContent.aspx"
        self.data = {}
        self.ocr = captcha.CaptchaRecognize()
        self.ocr.load_model()

    def get_captcha(self, src):
        captcha = self.rs.get(self.base_url + src, stream=True, verify=False)
        return captcha.content

    def get_captcha_src(self):
        req = self.rs.get(self.base_url + self.base_page)
        dom = BeautifulSoup(req.text, "html.parser")
        try:
            panel = dom.find("div", {"id":"Panel_bshtm"})
            img_src = panel.find('img')['src']
        except AttributeError:
            self.rs = requests.session()
            return self.get_captcha_src()
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
        status_code = BeautifulSoup(resp.text, 'html.parser')
        try:
            err = status_code.find('span', {'id':'Label_ErrorMsg'}).find('font').text
            if err.encode('utf-8') == "查無資料":
                print 'stock id %s invalid' % stock_id
                return None
            elif err != "":
                self.stock_ids.append(stock_id)
                return None
        except AttributeError:
            self.stock_ids.append(stock_id)
            return None
        resp = self.rs.get(self.base_url + self.csv_page)
        buf = io.StringIO(resp.text)
        for i in range(2):
            buf.readline()
        df = pd.read_csv(buf, encoding = 'utf8')
        req = self.rs.get(self.base_url + self.csv_page + "?v=t")
        dom = BeautifulSoup(req.text, 'html.parser')
        self.date = dateutil.parser.parse(dom.find('td', {'id':'receive_date'}).text)
        return df

    def stock_enumeration(self):
        while len(self.stock_ids) > 0:
            stock_id = self.stock_ids.pop(0)
            self.captcha = Image.open(io.BytesIO(self.get_captcha(self.get_captcha_src())))
            processed_captcha = self.ocr.preprocess(self.captcha)
            prediction = self.ocr.captcha_predict(processed_captcha)
            df = self.payload_once(prediction, stock_id)
            if df is not None:
                df = self.reshape_df(df)
                self.data[stock_id] = df
            self.rs = requests.session()
        return self.combine_data(self.data)

    def reshape_df(self, df):
        df.columns = ['index', 'broker_code', 'price', 'buy', 'sell', 'Unnamed', 'index.1', 'broker_code.1', 'price.1', 'buy.1', 'sell.1']
        firsthalf = df[['index', 'broker_code', 'price', 'buy', 'sell']]
        secondhalf = df[['index.1', 'broker_code.1', 'price.1', 'buy.1', 'sell.1']]
        secondhalf.columns = ['index', 'broker_code', 'price', 'buy', 'sell']
        return firsthalf.append(secondhalf)

    def combine_data(self, data):
        master_df = pd.DataFrame()
        for stock_id in data:
            df = data[stock_id]
            df['stock_id'] = stock_id
            master_df = master_df.append(df)
        master_df['datadate'] = self.date
        master_df = master_df.drop('index', 1)
        master_df = master_df[pd.notnull(master_df['broker_code'])]
        return master_df

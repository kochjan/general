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
import re
from PIL import Image
import captcha
from bs4 import BeautifulSoup
import pandas as pd
import os
import datetime
import dateutil

class TPEX:
    def __init__(self, stock_ids):
        self.rs = requests.session()
        self.base_url = "http://www.tpex.org.tw/web/stock/aftertrading/broker_trading/brokerBS.php?l=zh-tw"
        self.captcha_url = "http://www.tpex.org.tw/web/inc/authnum_output.php?n="
        self.stock_ids = stock_ids
        self.data = {}
        self.ocr = captcha.CaptchaRecognize()
        self.ocr.load_model()

    def csv_url_factory(self, stock_id, stock_date, prediction, enname):
        return "http://www.tpex.org.tw/web/stock/aftertrading/broker_trading/download_ALLCSV.php?curstk=%s&stk_date=%s&auth=%s&n=%s&charset=UTF-8" % (stock_id, stock_date, prediction, enname)

    def get_captcha(self, name):
        captcha = self.rs.get(self.captcha_url + name, stream=True, verify=False)
        return captcha.content

    def get_captcha_name(self):
        req = self.rs.get(self.base_url)
        dom = BeautifulSoup(req.text, "html.parser")
        name = dom.find("input", {"name": "enname"})['value']
        return name


    def payload_once(self, enname, prediction, stock_id):
        payload ={
            "enname" : enname,
            "stk_code" : stock_id,
            "auth_num" : prediction
            }
        resp = self.rs.post(self.base_url, data = payload)
        dom = BeautifulSoup(resp.text, 'html.parser')
        div = dom.find("div", {"class": "clearfix pt5 bg-warning"})
        data = re.findall(r"'([^']*)'" , div.find_all('button')[0]['onclick'])
        resp = self.rs.get(self.csv_url_factory(data[0], data[1], data[2], data[3]))
        date_str = str(data[1])
        date_str = date_str[-4:]
        date_str = date_str[:2] + "/" + date_str[2:]
        self.date = dateutil.parser.parse(date_str)
        buf = io.StringIO(resp.text)
        for i in range(2):
            buf.readline()
        try:
            df = pd.read_csv(buf, encoding = 'utf8')
        except pd.errors.ParserError:
            print "csv error: requeued id %s" % stock_id
            self.stock_ids.append(stock_id)
            return None
        return df

    def stock_enumeration(self):
        while len(self.stock_ids) > 0:
            stock_id = self.stock_ids.pop(0)
            try:
                enname = self.get_captcha_name()
                self.captcha = Image.open(io.BytesIO(self.get_captcha(enname)))
                processed_captcha = self.ocr.preprocess(self.captcha)
                prediction = self.ocr.captcha_predict(processed_captcha)
                df = self.payload_once(enname, prediction, stock_id)
                if df is not None:
                    df = self.reshape_df(df)
                    self.data[stock_id] = df
                self.rs = requests.session()
            except IOError:
                print "captcha error: requeued id %s" % stock_id
                self.stock_ids.append(stock_id)
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

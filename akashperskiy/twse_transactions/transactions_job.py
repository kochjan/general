#!/opt/python2.7/bin/python
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

import pandas as pd
import dateutil
from sqlalchemy import create_engine

TRANSACTIONS_URL = "http://dataeshop.twse.com.tw/frontend/en/product/downloadFile.jsp?ptid=A06&samplefile=ex-Excel-E.xls"

def generate_ten_minute_buckets():
    df = pd.read_excel(TRANSACTIONS_URL)
    df.columns = df.iloc[[0]].as_matrix()[0]
    df = df.iloc[1:]
    sums = df[df.columns[-7:]].astype(float).groupby(df.index//62).agg('sum')
    l = map(lambda x: x * 60, range(1, 28))
    columns = list(df.columns[-9:-7])
    columns.append(df.columns[1])
    avgs = df[columns].astype(float).groupby(df.index//62).mean()
    l = map(lambda x: x * 60, range(1, 28))
    avgs['Date'] = df.iloc[l][df.columns[0]].reset_index()[df.columns[0]].apply(dateutil.parser.parse)
    total = pd.concat([avgs, sums], axis=1)
    total.columns = ["raising", "decline", "taiex", "datadate", "order_bid", "bid_shares", "order_ask", "ask_shares", "transaction", "transaction_shares", "amount"]
    return total

if __name__ == "__main__":
    df = generate_ten_minute_buckets()
    engine = create_engine("mysql://nipun:Nipun123@130.211.136.197:3306/artur?charset=utf8")
    df.to_sql('twse_transactions', engine, if_exists="append", index=False)

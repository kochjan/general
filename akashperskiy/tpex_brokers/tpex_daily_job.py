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

from TPEX import TPEX
import pandas as import pd
from sqlalchemy import create_engine

def main():
    sl_df = pd.read_html("http://isin.twse.com.tw/isin/e_C_public.jsp?strMode=4")
    stock_list_df = sl_df[0]
    stock_list_df = stock_list_df[pd.notnull(stock_list_df[stock_list_df.columns[4]])]
    stock_ids = list(stock_list_df[stock_list_df.columns[0]].apply(lambda x: x[:4]))[1:]
    tpex = TPEX(stock_ids)
    result_df = tpex.stock_enumeration()
    engine = create_engine("mysql://nipun:Nipun123@130.211.136.197:3306/artur?charset=utf8")
    df.to_sql('tpex_brokers', engine, if_exists="append", index=False)

if __name__ == "__main__":
    main()

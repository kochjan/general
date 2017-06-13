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

import TWSE
import pandas as pd
from sqlalchemy import create_engine

def main():
    stock_list = pd.read_excel('http://dataeshop.twse.com.tw/frontend/en/product/downloadsampleFile.jsp?downloadfile=ECBIXLS/Z00_ECBI.xls')
    twse = TWSE.TWSE(list(stock_list['ECBI'])[1:])
    df = twse.stock_enumeration()
    engine = create_engine("mysql://nipun:Nipun123@130.211.136.197:3306/artur?charset=utf8")
    df.to_sql('testtaiwan', engine, if_exists="append", index=False)

if __name__ =="__main__":
    main()

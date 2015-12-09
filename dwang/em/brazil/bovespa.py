#!/opt/python2.7/bin/python

import pandas as pd
from datetime import datetime as dd
import requests
import bs4 as bs
import re
import sys
import time
import os
import sql_io
import re

s = requests.Session()

outdir = './outputs'
urlbase = 'http://www.bmfbovespa.com.br/BancoTitulosBTC'
BTCurl = urlbase + '/PosicoesEmAberto.aspx?Idioma=en-us'

req = s.get(BTCurl)
soup= bs.BeautifulSoup(req.content)

viewstate =soup.find(type='hidden', id='__VIEWSTATE').get('value')
eventVal =soup.find(type='hidden', id='__EVENTVALIDATION').get('value')

headers = {'User-Agent': 'Mozilla/5.0', 
           'Connection': 'keep-alive',
           'Content-Type': 'application/x-www-form-urlencoded',
           'Accept-Encoding': 'gzip, deflate',
           'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
           }


payload = {'__EVENTTARGET':'', '__EVENTARGUMENT':'', 
           '__VIEWSTATE': viewstate,
           '__EVENTVALIDATION':eventVal,
           'ctl00$contentPlaceHolderConteudo$acoes$txtConsultaEmpresa':'',
           'ctl00$contentPlaceHolderConteudo$acoes$btnBuscarArquivos':'Search'
}

datevar = 'ctl00$contentPlaceHolderConteudo$acoes$txtConsultaDataDownload$txtConsultaDataDownload$dateInput'
dateval = '{:%Y-%m-%d}-00-00-00'

def addDateToPayload(date):
    payload[datevar] = dateval.format(date)

def downloadBTCDateRange(start, end):
    fns = []
    for d in pd.DateRange(start, end):
        addDateToPayload(d)
        req2 = s.post(BTCurl,headers=headers,data=payload)
        
        res = req2.headers.get('content-disposition')
        if pd.isnull(res):
            print "no data on {}".format(d)
        elif 'filename' in res:
            filename = res.split('=')[1]
    	    print filename
            filepath = '{}/BTC-OpenPositions/{}'.format(outdir,filename)
            foldern = outdir+'/BTC-OpenPositions'
            if not os.path.exists(foldern):
                os.makedirs(foldern)
            with open(filepath, "wb") as f:
                f.write(req2.content)
        # pause 5 secs
        fns.append(filepath)
        time.sleep(5)

    return fns

RLCurl = urlbase + '/EmprestimoRegistrado.aspx?idioma=en-us&download=planilha'

def downloadRLC():
    
    req3 = s.get(RLCurl)
    res = req3.headers.get('content-disposition')
    
    if pd.isnull(res):
        print "no data on {}".format(d)
        return
    elif 'filename' in res:
        filename = res.split('=')[1].split('.')[0]
        filepath = '{}/RLC-RegisteredLoanContract/{}_{:%Y%m%d}.txt'.format(outdir, filename, dd.today())
        foldern = outdir+'/RLC-RegisteredLoanContract'
        if not os.path.exists(foldern):
            os.makedirs(foldern)
        with open(filepath, "wb") as f:
            f.write(req3.content)
    #time.sleep(5)
    return filepath

# getting historical 2 months
# downloadBTCDateRange(dd(2015,7,11), dd(2015,8,1))

def BTC_parser(fns):#DBTC20151207.dat

    tmp_l = []
    cols = ['security_type', 'ticker', 'name', 'paper_type', 'isin', 'contract_balance', 'price', 'role', 'balance_real', 'datadate', 'reseve_booking']
    for fn in fns:
        with open(fn, 'r') as f:
            f_list = f.readlines()
        for l in f_list[1:-1]:
            security_type = l[0:2]#'01': equity; '02': fixed income
            if (security_type=='01') or (security_type=='02'):
                ticker = l[2:14]
                name = l[14:26]
                paper_type = l[26:29]
                isin = l[29:41]
                contract_balance = l[41:59]#balance of contract on paper: QUANTITY OF THE SUM TOTAL OF CONTRACTS OPEN IN DATE MOVEMENT
                price = l[59:77]#price in the middle of data movement
                role = l[77:84]#ROLE OF LISTING FACTOR
                balance_real = l[84:102]#BALANCE OF CONTRACT IN REAL SUM OF VOLUME: SUM OF VOLUME IN REAL , THE CONTRACTS OPEN IN DATE MOVEMENT
                reseve_booking = l[102:250]
            else:
                raise Exception('bad security_type')

            datadate = re.findall('DBTC(\d*)\.', fn)[0]
            tmp_l.append([k.strip() for k in [security_type, ticker, name, paper_type, isin, contract_balance, price, role, balance_real, datadate, reseve_booking]])

    df = pd.DataFrame(tmp_l, columns=cols)
    
    return df
    

def RLC_parser(fns):

    tmp_l = []
    cols = ['record_type', 'days', 'action', 'name', 'contract_num', 'shares', 'value', \
            'min_rate_loan', 'avg_rate_loan', 'max_rate_loan', 'min_rate_borrow', 'avg_rate_borrow', \
            'max_rate_borrow', 'datadate', 'reserve']
    for fn in fns:
        with open(fn, 'r') as f:
            f_list = f.readlines()
        for l in f_list[1:-1]:
            record_type = l[0:2]
            if (record_type=='01'):
                action = l[2:22]
                name = l[22:52]
                contract_num = l[52:62]
                shares = l[62:73]
                value = l[73:93]
                min_rate_loan = l[93:100]
                avg_rate_loan = l[100:107]
                max_rate_loan = l[107:114]
                min_rate_borrow = l[114:121]
                avg_rate_borrow = l[121:128]
                max_rate_borrow = l[128:135]
                reserve = l[135:160]
                days = '1'
            elif (record_type=='02'):
                action = l[2:22]
                name = l[22:52]
                contract_num = l[52:62]
                shares = l[62:73]
                value = l[73:93]
                avg_rate_loan = l[93:100]
                avg_rate_borrow = l[100:107]
                reserve = l[135:160]
                min_rate_loan = ''
                max_rate_loan = ''
                min_rate_borrow = ''
                max_rate_borrow = ''
                days = '3'
            elif (record_type=='03'):
                action = l[2:22]
                name = l[22:52]
                contract_num = l[52:62]
                shares = l[62:73]
                value = l[73:93]
                avg_rate_loan = l[93:100]
                avg_rate_borrow = l[100:107]
                reserve = l[135:160]
                min_rate_loan = ''
                max_rate_loan = ''
                min_rate_borrow = ''
                max_rate_borrow = ''
                days = '15'
            else:
                raise Exception('bad security_type')
            
            datadate = re.findall('\_(\d*)\.', fn)[0]
            tmp_l.append([k.strip() for k in [record_type, days, action, name, contract_num, shares, value, \
            min_rate_loan, avg_rate_loan, max_rate_loan, min_rate_borrow, avg_rate_borrow, \
            max_rate_borrow, datadate, reserve]])

    df = pd.DataFrame(tmp_l, columns=cols)
    return df

if __name__ == '__main__':
    day = dd.today() - pd.datetools.BDay(1)
    print "processing {}".format(day)
    fns = downloadBTCDateRange(day, day)
    BTC_df = BTC_parser(fns)
    sql_io.write_frame(BTC_df, 'dwang..brazil_bovespa_BTC', bulk='off', if_exists='append')
    # BTC available up to previous Friday?

    fn = downloadRLC()
    RLC_df = RLC_parser([fn])
    sql_io.write_frame(RLC_df, 'dwang..brazil_bovespa_RLC', bulk='off', if_exists='append')

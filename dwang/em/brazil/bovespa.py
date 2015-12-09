#!/opt/python2.7/bin/python

import pandas as pd
from datetime import datetime as dd
import requests
import bs4 as bs
import re
import sys
import time
import os

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
        filepath = '{}/RLC-RegisteredLoanContract/rlc.{:%Y%m%d}.txt'.format(outdir, dd.today())
        foldern = outdir+'/RLC-RegisteredLoanContract'
        if not os.path.exists(foldern):
            os.makedirs(foldern)
        with open(filepath, "wb") as f:
            f.write(req3.content)
    #time.sleep(5)
    return filepath

# getting historical 2 months
# downloadBTCDateRange(dd(2015,7,11), dd(2015,8,1))

def BTC_parser(fns):

    tmp_l = []
    cols = ['registration_type', 'code', 'name', 'paper_type', 'isin', 'balance_paper', 'price', 'role', 'balance_real', 'reseva_booking']
    for fn in fns:
        with open(fn, 'r') as f:
            f_list = f.readlines()
        for l in f_list[1:-1]:
            registration_type = l[0:2]
            code = l[2:14]
            name = l[14:26]
            paper_type = l[26:29]
            isin = l[29:41]
            balance_paper = l[41:59]
            price = l[59:77]
            role = l[77:84]
            balance_real = l[84:102]
            reseva_booking = l[102:250]

            tmp_l.append([k.strip() for k in [registration_type, code, name, paper_type, isin, balance_paper, price, role, balance_real, reseva_booking]])

    df = pd.DataFrame(tmp_l, columns=cols)
    return df
    

def RLC_parser(fns):

    tmp_l = []
    cols = ['registration_type', 'code', 'name', 'paper_type', 'isin', 'balance_paper', 'price', 'role', 'balance_real', 'reseva_booking']
    for fn in fns:
        with open(fn, 'r') as f:
            f_list = f.readlines()
        for l in f_list[1:-1]:
            registration_type = l[0:2]
            action = l[2:22]
            now = l[22:52]???stoped here
            paper_type = l[26:29]
            isin = l[29:41]
            balance_paper = l[41:59]
            price = l[59:77]
            role = l[77:84]
            balance_real = l[84:102]
            reseva_booking = l[102:250]

            tmp_l.append([k.strip() for k in [registration_type, code, name, paper_type, isin, balance_paper, price, role, balance_real, reseva_booking]])

    df = pd.DataFrame(tmp_l, columns=cols)
    return df

if __name__ == '__main__':
    day = dd.today() - pd.datetools.BDay(1)
    print "processing {}".format(day)
    fns = downloadBTCDateRange(day, day)
    BTC_df = BTC_parser(fns)
    # BTC available up to previous Friday?

    fn = downloadRLC()
    RLC_df = RLC_parser(fn)

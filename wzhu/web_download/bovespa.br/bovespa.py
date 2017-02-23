#!/opt/python2.7/bin/python

import pandas as pd
from datetime import datetime as dd
import requests
import bs4 as bs
import re
import sys
sys.path.insert(0,'/home/wzhu/gitlabs/data-collection/utils')
from common import pause

s = requests.Session()

outdir = '/home/wzhu/download_daily/bovespa.bz'
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
	    with open(filepath, "wb") as f:
		f.write(req2.content)
	# pause 5 secs
	pause()

RLCurl = urlbase + '/EmprestimoRegistrado.aspx?idioma=en-us&download=planilha'

def downloadRLC():
    req3 = s.get(RLCurl)
    res = req3.headers.get('content-disposition')
    if pd.isnull(res):
        print "no data on {}".format(d)
    elif 'filename' in res:
        filepath = '{}/RLC-RegisteredLoanContract/rlc.{:%Y%m%d}.txt'.format(outdir, dd.today())
	with open(filepath, "wb") as f:
	    f.write(req3.content)
    pause()

# getting historical 2 months
# downloadBTCDateRange(dd(2015,7,11), dd(2015,8,1))

if __name__ == '__main__':
    day = dd.today() - pd.datetools.BDay(1)
    print "processing {}".format(day)
    downloadBTCDateRange(day, day)
    # BTC available up to previous Friday?

    downloadRLC()

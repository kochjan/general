#!/opt/anaconda/py/bin/python
import pandas as pd
from datetime import datetime as dd
import re
import time
import requests
import bs4 as bs
s = requests.Session()

outdir = '/home/wzhu/download_daily/jpx.shortselling.industry/'
base = 'http://www.jpx.co.jp/english/markets/statistics-equities/short-selling/'

# Index
def getIndexUrl(n):
    if n < 1:
        return base + "index.html"
    paddedN = n
    if (n<10):
        paddedN = '0{}'.format(n)
    return base + '00-archives-{}.html'.format(paddedN)

def downloadIndex(N):
    r = s.get(getIndexUrl(N))
    b = bs.BeautifulSoup(r.content)
    pdflinks = b.findAll('a', href=re.compile('ge.pdf') )

    for pl in pdflinks:
	link = pl['href']
        filename = link.split('/')[-1]
	print filename
	req = s.get('http://www.jpx.co.jp'+link)
	print '.'
        with open(outdir+filename, 'wb') as OFH:
	    OFH.write(req.content)
    
        time.sleep(2)

for n in range(1,12):
    downloadIndex(n)
    

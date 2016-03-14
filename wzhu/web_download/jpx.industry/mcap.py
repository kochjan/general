#!/opt/anaconda/py/bin/python
import pandas as pd
from datetime import datetime as dd
import re
import time
import requests
import bs4 as bs
s = requests.Session()

outdir = '/home/wzhu/download_daily/jpx.shortselling.industry/mcap/'
url = 'http://www.jpx.co.jp/english/markets/statistics-equities/misc/02-01.html'

r = s.get(url)
b = bs.BeautifulSoup(r.content)
pdflinks = b.findAll('a', href=re.compile('.pdf') )

for pl in pdflinks:
    link = pl['href']
    filename = link.split('/')[-1]
    print filename
    req = s.get('http://www.jpx.co.jp'+link)
    print '.'
    with open(outdir+filename, 'wb') as OFH:
	OFH.write(req.content)
    
    time.sleep(2)


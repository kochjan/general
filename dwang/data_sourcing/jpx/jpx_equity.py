#!/opt/python2.7/bin/python2.7

from bs4 import BeautifulSoup
import requests
import time

url_base = "http://www.jpx.co.jp/"
url = "http://www.jpx.co.jp/english/markets/statistics-equities/investor-type/00-00-archives-%s.html"

for i in range(0, 11):
    r = requests.get(url%('%.2d'%i)).content
    s = BeautifulSoup(r)
    AS = s.findAll('a')

    for f in [i.get('href') for i in AS if '.xls' in i.get('href', '')]:
        #import pdb; pdb.set_trace()
        fr = requests.get(url_base+f).content
        fn = f.split('/')[-1]
        print fn
        with open('./%s'%fn, 'w') as xlsf:
            xlsf.write(fr)

        time.sleep(30)
        
    

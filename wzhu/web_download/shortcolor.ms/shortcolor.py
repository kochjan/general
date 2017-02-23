#!/opt/python2.7/bin/python
import pandas as pd
from datetime import datetime as dd
import requests
import bs4 as bs

DEFAULT_OUTDIR_BASE = '/home/wzhu/download_daily/shortcolor.ms'
s = requests.Session()

def run(date):
    #yesterday = date - pd.DateOffset(1)
    ymd = '{:%Y%m%d}'.format(date)
    print ymd

    s2 = s.get('https://secure.ms.com/sltny/sltLMS/webapp/mc?date={}'.format(ymd), 
	auth=('marketcolor@nipuncapital.com','abc1234'))

    soup2=bs.BeautifulSoup(s2.content, 'xml')


    # [c.name for c in soup2.Sections.children]
    # ['Contributions', 'TopCoversByMV', 'TopDTC', 'TopShortsByMV', 'TopShortsByPopularity']
    # print list(soup2.TopCoversByMV.children)[1].prettify()

    if pd.isnull(soup2.Contributions): # or (len(soup2.Contributions.children) < 1):
	print "empty day"
	return None

    # 1st category is not instrument specific
    outfile = '{}/content.{}.1.csv'.format(DEFAULT_OUTDIR_BASE, ymd)
    outFH = open(outfile,'w')
    for c in soup2.Contributions.children:
	outFH.write("{},{},{},{},{},{}\n".format(ymd, 'Contributions', c.category.text.strip(), c.contribution.text, c.region.text, c.sector.text))
    outFH.close()

    # next 4 categories are top list of instruments
    outfile = '{}/content.{}.4.csv'.format(DEFAULT_OUTDIR_BASE, ymd)
    outFH = open(outfile,'w')

    for c in soup2.TopCoversByMV.children:
        outFH.write("{},{},{},{},{},{},{},{},{}\n".format(ymd, 'TopCoversByMV', c.category.text.strip(), c.sedol.text, c.cusip.text, c.rank.text, c.region.text, c.country.text, c.sector.text))

    for c in soup2.TopDTC.children:
	outFH.write("{},{},{},{},{},{},{},{},{}\n".format(ymd, 'TopDTC', c.category.text.strip(), c.sedol.text, c.cusip.text, c.rank.text, c.region.text, c.country.text, c.sector.text))

    for c in soup2.TopShortsByMV.children:
	outFH.write("{},{},{},{},{},{},{},{},{}\n".format(ymd, 'TopShortsByMV', c.category.text.strip(), c.sedol.text, c.cusip.text, c.rank.text, c.region.text, c.country.text, c.sector.text))

    for c in soup2.TopShortsByPopularity.children:
	outFH.write("{},{},{},{},{},{},{},{},{}\n".format(ymd, 'TopShortsByPopularity', c.category.text.strip(), c.sedol.text, c.cusip.text, c.rank.text, c.region.text, c.country.text, c.sector.text))

    outFH.close()
    return '{}/content.{}'.format(DEFAULT_OUTDIR_BASE, ymd)

import nipun.dbc as db
import pandas as pd
import time
import subprocess
import nipun.cpa.load_barra as lb
UNIVERSE = 'npxchnpak'

OPT_DIR = "/opt/production/current/optimizer/npxchnpak"

def saveCSV(date):
    sql = '''SELECT isnull(tm.barrid, sm.barrid) as barrid, sm.sedol, sm.name, sm.localid, 
  sm.listed_country, s4.datadate, s4.category, s4.rank, s4.region, s4.country, s4.sector
  FROM [wzhu].[dbo].[shortcolor4] s4 join [nipun_prod].[dbo].[security_master] sm
  on s4.datadate between sm.datadate and isnull(sm.stopdate, '2050-01-01')
  and s4.sedol = sm.sedol
  and s4.section = 'TopDTC' and s4.region in ('AS', 'JP')
  and s4.datadate = '{:%Y%m%d}'
  left join nipun_prod..thai_barrid_map tm on left(sm.barrid, 6)=tm.root_id
    '''.format(date)
    dbo = db.db(connect='qai')
    data = dbo.query(sql, df=True)

    data = data.sort(['barrid','rank'])
    data = data.drop_duplicates(['barrid'], take_last=False)    
    data = data.sort(['category','region','rank'])

    univ = lb.load_production_universe(UNIVERSE, date).index
    data = data[map(lambda x: x in univ, data['barrid'])]    

    fout = "{}/shortcolorDTC.{:%Y%m%d}.csv".format(OPT_DIR, date)
    data.to_csv(fout, index=False, columns=['barrid','sedol','name','localid','listed_country','datadate','category','region','rank','sector'])

if __name__ == '__main__':
    #for d in pd.DateRange( dd(2015,7,30), dd(2015,8,3)):
    #	run(d)
    d = dd.today() - pd.datetools.BDay(1)
    filebase = run(d)
    if filebase:
	time.sleep(5)
	print subprocess.check_output(["freebcp", "wzhu.dbo.shortcolor1","in", filebase+".1.csv","-S","192.168.50.6", "-U", "nipun", "-P", "Nipun123", "-t,", "-c"])
	print subprocess.check_output(["freebcp", "wzhu.dbo.shortcolor4","in", filebase+".4.csv","-S","192.168.50.6", "-U", "nipun", "-P", "Nipun123", "-t,", "-c"])
	time.sleep(5)
	saveCSV(d)

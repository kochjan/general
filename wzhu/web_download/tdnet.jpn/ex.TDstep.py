cd tasks/201502.tdnet.hist/
from TDstep import *
init()
enterJapan()
reqA1=enterTab()
soup(reqA1)

q1 = getAllStockQuery(dd(2010,3,10),dd(2010,3,20))
reqB1 = enterQuery(q1)
soupB1=soup(reqB1)

cd ../201501.sites/
import treeViewer
cd ../201502.tdnet.hist/
treeViewer.traverse(soupB1.contents[3],3,1)
tbody = treeViewer.getIx(soupB1, [3,3,1,5,4,7,11,3])
assert(tbody.name == 'tbody'), 'structure doesnt lead to tbody. need recode'

# get content
b1=soupB1.find('body')
f1=b1.find('form', attrs={'name':'TDN010020Form'})
d1= f1.find('div', attrs={'id':'main-contents'})
d2=d1.find('div', attrs={'id':'wrap'})

#paging
p1=d2.find('div', attrs={'id':'paging'})
pMax=p1.find('input', attrs={'type':'hidden', 'name':'pageMax'}).get('value')

# faster : using find, we should first assert their uniqueness
pageMax = soupB1.find('input', attrs={'type':'hidden', 'name':'pageMax'}).get('value')
r1=soupB1.find('table', attrs={'class':'results'}).find('tbody')
for tag in r1.contents:
    badAttempts = '''
    fields = r3.findAll('input')
    meanings = ['DocId', 'datetime', 'localCode', 'firmName','titleText', 
    patterns = ['kijNo','kijDtm','mgrCd', 'egMgrNm', 'kkiKmkOut',
    '''
    if (type(tag) != BeautifulSoup.Tag):
	continue

    kijNo = r3.find('input', attrs={'type':'hidden', 'name': re.compile("kijNo$")}).get('value')
    fieldRE = map(re.compile, ['kijNo$','kijDtm$','mgrCd$', 'egMgrNm$'])

    should just save every single type="hidden" field value and name:

r33=r4[0].findAll('input')

[<input type="hidden" name="ccTdn010020LstDspNiy_st[0].kijNo" value="20100311041341" />,
 <input type="hidden" name="ccTdn010020LstDspNiy_st[0].kijDtm" value="2010/03/19 22:10" />,




q2 = get3KeywordQuery(dd(2011,3,10),dd(2011,3,20))
reqB2 = enterQuery(q2)
soupB2= soup(reqB2)


LO=logout()
soup(LO)


# test runGeneral:
cd /home/wzhu/tasks/201502.tdnet.hist/
from TDstep_v2 import *
init()
enterJapan()
enterTab()
soup(_)

runGeneral(dd(2015,3,3), True, '/home/wzhu/download_daily/tdnet')

run(date) same as 
runGeneral(dd(2010,3,16), False, '/home/wzhu/download_daily/tdnet')

for rdate in pd.DateRange(dd(2010,4,1), dd(2010,6,30), offset=pd.datetools.day):
    runGeneral(rdate, True, '/home/wzhu/download_daily/tdnet')
    time.sleep(5)

runGeneral(dd(2010,4,1), True, '/home/wzhu/download_daily/tdnet')

for rdate in range(3,30):
    runGeneral(dd(2010,4,rdate), True, '/home/wzhu/download_daily/tdnet')
    time.sleep(6.5)

for rdate in pd.DateRange(dd(2010,8,17), dd(2011,1,1), offset=pd.datetools.day):
    runGeneral(rdate, True, '/home/wzhu/download_daily/tdnet')
    time.sleep(5)

    fix: sleep(0.1)

import urllib
url = 'https://www.release.tdnet.info/inbs/140120150122009439.pdf'
url = 'https://www.release.tdnet.info/inbs/081220150128013027.zip'
s.get(url)

# test simple download
def downloadTitleLink(x):
    '''download title with id=x'''
    link = pageA
    header2['Referer'] = pageA
    qstring= 'tshShti=58&TitleLink=TitleLink'
    reqNextPage = s.post(link, headers=header2, data=qstring, cookies=s.cookies)
    return reqNextPage

    qstring= 'tshShti=56&Dwnlad=Dwnlad'

TitleLink=TitleLink

resTag = getResultTag(soupB1)
<input type="hidden" name="ccTdn010020LstDspNiy_st[19].kijNo" value="20100319049110" />
<input type="hidden" name="ccTdn010020LstDspNiy_st[19].hydiLnkPsCd" value="58" />
csv:
    21  20100319049110,2010/03/19 15:00,97430,,??22??????????????????????<,true<,58,,,(350)(??)??????????<,true<,,???,,,,

reqNextPage.headers
Out[125]: CaseInsensitiveDict({'content-length': '82372', 'content-disposition': 'inline; filename = 140120100319049110.pdf', 'server': 'Apache', 'connection': 'close', 'date': 'Wed, 18 Mar 2015 23:48:35 GMT', 'content-type': 'application/pdf'})


# Test download PDF/zip:

#cd /home/wzhu/tasks/201502.tdnet.hist/
#from TDstep_v2 import *
cd /home/wzhu/gitlabs/data-collection/tdnet.jpn
from tdnet_history import *

init()
enterJapan()
enterTab()

allDoc = True
keywordQ= False
patch = True

# Keyword PDF/ZIP download
runGeneral(dd(2010,3,19), keywordQ, '/home/wzhu/download_daily/tdnet')

for rdate in pd.DateRange(dd(2010,7,1), dd(2011,1,1), offset=pd.datetools.day):
    runGeneral(rdate, keywordQ)
    time.sleep(3)

fix:
for rdate in pd.DateRange(dd(2010,5,6), dd(2010,5,17), offset=pd.datetools.day):
    runGeneral(rdate, keywordQ)
    time.sleep(3)

runGeneral(dd(2010,7,31), keywordQ)
runGeneral(dd(2010,10,23), keywordQ)

for rdate in pd.DateRange(dd(2013,6,20), dd(2015,2,1), offset=pd.datetools.day):
    runGeneral(rdate, keywordQ)
    time.sleep(3)

# read from list instead:
import commands
d = commands.getoutput('ls /home/wzhu/download_daily/tdnet/history/*/*/*Again').split("\n")
ndates = map(lambda x: dd.strptime(x.split('.')[1], '%Y%m%d'), d)

for ndd in ndates:
    runGeneral(ndd, keywordQ)

# to check the rerun:
for f in */*/*ain*; do echo $f | sed 's/[^\/]*$//'| xargs ls -l | grep csv; done
#All good
junk */*/*ain*


# AllDoc historical
allDoc = True

for rdate in pd.DateRange(dd(2010,3,19), dd(2015,2,1), offset=pd.datetools.day):
    runGeneral(rdate, allDoc)
    time.sleep(3)

# search again:
import commands
d = commands.getoutput('ls /home/wzhu/download_daily/tdnet/history.all/*/*/*Again').split("\n")
ndates = map(lambda x: dd.strptime(x.split('.')[2], '%Y%m%d'), d)

for ndd in ndates:
    runGeneral(ndd, allDoc)
    time.sleep(3)

# search again can be iterated to redo the missing ones during first past

#patching:
runGeneral(dd(2010,3,30), allDoc, '/home/wzhu/download_daily/tdnet', patch)
runGeneral(dd(2010,3,31), allDoc, '/home/wzhu/download_daily/tdnet', patch)
runGeneral(dd(2010,5,13), allDoc, '/home/wzhu/download_daily/tdnet', patch)

# read from list instead:
import commands
d = commands.getoutput('wc -l /home/wzhu/download_daily/tdnet/history.all/*/*/*null').split("\n")
nd = [x for x in d if (x.find('1001')>-1) ]
ndates = map(lambda x: dd.strptime(x.split('.')[2], '%Y%m%d'), nd)

for ndd in ndates:
    runGeneral(ndd, allDoc, '/home/wzhu/download_daily/tdnet', patch)

# some may do "search again" error: 
# need to remove the *SearchAgain file 
# and mv the null.2 file back to null:
cd 201305/20130514
junk *searchAgain
mv queryResult.20130514.csv.cp10.mp11.null{.2,} 
# then ready to redo via the above automatic version, or do it manually as next stanza:

fix:
runGeneral(dd(2010,6,29), allDoc, '/home/wzhu/download_daily/tdnet', patch)
runGeneral(dd(2011,2,10), allDoc, '/home/wzhu/download_daily/tdnet', patch)
runGeneral(dd(2010,5,13), allDoc, '/home/wzhu/download_daily/tdnet', patch)
runGeneral(dd(2011,5,13), allDoc, '/home/wzhu/download_daily/tdnet', patch)
runGeneral(dd(2011,6,29), allDoc, '/home/wzhu/download_daily/tdnet', patch)



exit/quit: logout()

def ex():
    logout()
    exit




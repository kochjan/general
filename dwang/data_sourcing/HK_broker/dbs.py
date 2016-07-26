#!/opt/python2.7/bin/python2.7
"""
need to create an object of the class KGI, then call run_all
only keep the function run untouched, but override all the other functions:
gen_list
__init__
run_all
parse_page
upload_to_DB
"""
import codecs
from web_scraper import *
def getEntryData(entry, fieldName):
    try:
        dat = entry.find('span',{'class':fieldName}).text.split(u'\xa0')[-1].strip()
    except:
        dat = ''
    return dat
    
infolist = ['sector','status','sector-area','sector-name','stock-code','target-price','m-cap']

    
class Dbs(WebScraper):

    def __gen_list__(self):
        sess = requests.Session()
        sess.get('https://www.dbs.com.hk/treasures/aics/grid.page', verify=False)
        req = sess.post('http://www.dbs.com.hk/treasures/aics/gridchild.page', verify=False,
            data={'country':'hk', 'sector':'all', 'recommentation':'all', 'start':0,'search':'','pageNum':1,'clsVal':''})  
        soupCurr = BeautifulSoup(req.content)
        pageTotal=len(soupCurr.find('div',{'class':'pagination'}).findAll('a',{'href':'#'}))-2
        self.session = sess
        base_url = 'https://www.dbs.com.hk/treasures/aics/gridchild.page?start='
        return [base_url+str(i) for i in xrange(1,pageTotal+1)]
        
    def scrape_sig_page(self, url):
        base_url = 'https://www.dbs.com.hk/treasures/aics/gridchild.page'
        i = url.split("=")[-1]
        page = self.session.post(base_url, data={'country':'hk', 'sector':'all', 'recommentation':'all', 'search':'','start':i}, verify=False)
        #'submit':'true','componentID':'1432599052736',         
        return page.content

    def parse_page(self):

        rec_map = {'OW':'Overweight', 'N':'Neutral', 'UW':'Underweight', 'FV': 'Fully Valued'}
        tmp_l = []
        s = BeautifulSoup(self.page_str)
        with codecs.open('bs.out','w', 'utf-8') as f:
            f.write(s.prettify())
            
        entries=s.findAll('div', {'class':"stocks-popover"})
        
        for entry in entries:
            entryData = map(lambda x: getEntryData(entry, x), infolist)
            data = dict(zip(infolist, entryData))
            ticker = data.get("stock-code")
            if 'HK' in ticker:
                ticker = ticker.replace('.HK','')
            else:
                continue
            
            recommendation = rec_map[data.get("status")]
            tgt_price = data.get("target-price").replace('HK$','').strip() # default is HK$
            time_per = data.get("time-estimate")
            
            datadate = dt.datetime.combine(dt.datetime.today(), dt.time())
            tmp_l.append([datadate, ticker, 'recommendation', recommendation])
            tmp_l.append([datadate, ticker, 'time', time_per])
            tmp_l.append([datadate, ticker, 'target price', tgt_price])
        
        
        self.res_df = pd.DataFrame(tmp_l, columns=['datadate', 'ticker', 'category', 'value'])
        self.res_df['source'] = 'dbs'
        self.res_df['last_modification'] = dt.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        self.res_df['sm_localid'] = self.res_df['ticker'].apply(lambda x: 'HK'+str(int(x)))

        #self.res_df = pd.merge(self.res_df, bar_localid_df, left_on='sm_localid', right_on='localid', how='left')[['datadate','barrid', 'category', 'value', 'ticker', 'source', 'last_modification']]
        self.res_df = match_barrid(self.res_df, bar_localid_df)[['datadate','barrid', 'category', 'value', 'ticker', 'source', 'last_modification']]
        self.res_df = self.res_df.drop_duplicates()
        self.prt(self.res_df)

if __name__ == '__main__':
    
    opts, args = parse()
    obj = Dbs(opts.stdout)
    obj.run_all(opts.db, opts.forceall)
    if opts.email: 
        obj.email()       
        

        

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

from web_scraper import *

class Poems2(WebScraper):

    def __gen_list__(self):

        base_url = 'http://www.poems.com.hk/base/DocumentService/ResearchReportTreeData'
        base_url2 = 'http://www.poems.com.hk/'
        href_list = []
        l = [(i.year, i.month) for i in pd.DateRange(dt.datetime(2006,2,1), pd.datetools.thisMonthEnd(dt.datetime.today()), offset=pd.datetools.monthEnd)]
        #l = [(i.year, i.month) for i in pd.DateRange(dt.datetime(2015,2,1), dt.datetime(2015,5,1), offset=pd.datetools.monthEnd)]
        l.reverse()
        for y,m in l:
            s = requests.post(base_url, data={'lang':'en-US','pid':3089,'mode':'children','parent':'y=%s&m=%s'%(y,m)}).content
            tmp_list = re.findall('({[\w\W]*?})', s)
            for rpt in tmp_list:
                yield base_url2+re.search('href=.*?"(.*?)"', rpt).groups()[0]

    def parse_page(self):
        
        s = BeautifulSoup(self.page_str)
        
        ## title = s.find('h1', {'class':'document-title'}).text.strip()
##         tmp_o = re.findall('[\(<](.*?)[\)>]', title)
##         if tmp_o is None:#no ticker in the title
##             self.res_df = pd.DataFrame()
##             return
        
##         ticker = None
##         for i in tmp_o:
##             if i.isdigit():
##                 ticker = i
##                 break
##             elif 'HK' in i:
##                 ticker = i.replace('H','').replace('K','').replace('.','').replace(',','').encode('ascii', errors='ignore')
##                 break
##         if ticker is None:#no ticker in the title
##             self.res_df = pd.DataFrame()
##             return

        ## if ticker[-2:].isdigit() or ticker[-2:]=='HK':
##             ticker = ticker.split('.')[0]
##         else:#it is .CH or .SH, not HK local stock
##             self.res_df = pd.DataFrame()
##             return

        title = s.find('div', {'class':"companycode-title"}).text.strip()
        ticker = re.search('[\(](\d+)[\)]', title).groups()[0]

        rec = s.find('table', {'class':'document-recommend'})
        ## if not rec:
##             self.res_df = pd.DataFrame()
##             return
        datadate = re.search('\d* [\w]* \d*', rec.find('caption').text).group()
        datadate = dt.datetime.strptime(datadate, '%d %B %Y')

        try:
            recommendation = rec.find('td', {'class':'document-recommand-suggestion text-align-right'}).text
        except Exception, e:
            recommendation = None

        #import pdb; pdb.set_trace()
        rec_trs = rec.findAll('tr')
        tgt_price = None
        sgp_price = None
        for i in rec_trs:
            try:
                if 'target price' in i.text.strip().lower():
                    tgt_price = i.findAll('td', {'class':'document-recommand-value text-align-right'})[-1].text
                    tgt_price = tgt_price.split('$')[-1]
            except Exception, e:
                print e
            try:
                if 'suggested purchase price' in i.text.strip().lower():
                    sgp_price = i.findAll('td', {'class':'document-recommand-value text-align-right'})[-1].text
                    sgp_price = sgp_price.split('$')[-1]
            except Exception, e:
                print e
        ## try:
##             tgt_price = rec.findAll('td', {'class':'document-recommand-value text-align-right'})[-1].text
##             tgt_price = tgt_price.split('$')[-1]
##         except Exception, e:
##             tgt_price = None

        tmp_l = []
        if recommendation:
            tmp_l.append([datadate, ticker, 'recommendation', recommendation])
        if tgt_price:
            tmp_l.append([datadate, ticker, 'target price', tgt_price])
        if sgp_price:
            tmp_l.append([datadate, ticker, 'suggested purchase price', sgp_price])
        ## if len(tmp_l)==0:
##             self.res_df = pd.DataFrame()
##             return

        self.res_df = pd.DataFrame(tmp_l, columns=['datadate', 'ticker', 'category', 'value'])
        self.res_df['source'] = self.__class__.__name__.lower()
        self.res_df['last_modification'] = dt.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        self.res_df['sm_localid'] = self.res_df['ticker'].apply(lambda x: 'HK'+str(int(x)))

        #self.res_df = pd.merge(self.res_df, bar_localid_df, left_on='sm_localid', right_on='localid', how='left')[['datadate','barrid', 'category', 'value', 'ticker', 'source', 'last_modification']]
        self.res_df = match_barrid(self.res_df, bar_localid_df)[['datadate','barrid', 'category', 'value', 'ticker', 'source', 'last_modification']]
        self.res_df = self.res_df.drop_duplicates()
        self.prt(self.res_df)

if __name__ == '__main__':

    try:
        opts, args = parse()
        obj = Poems2(opts.stdout)
        obj.run_all(opts.db, opts.forceall)
    finally:
        if not opts.stdout:
            obj.email()
        
            

        

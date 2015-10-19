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

class Dbs(WebScraper):

    def __gen_list__(self):

        base_url = 'http://www.dbs.com.hk/treasures/aics/gridchild.page?country=hk&sector=all&recommentation=all&search=&start='
        return [base_url+str(i) for i in xrange(1,4)]
        
    ## def scrape_sig_page(self, i):
        
##         base_url = 'https://www.dbs.com.hk/treasures/aics/gridchild.page'
##         return requests.post(base_url, data={'country':'hk', 'sector':'all', 'recommentation':'all', 'start':i}).content

    def parse_page(self):

        rec_map = {'OW':'Overweight', 'N':'Neutral', 'UW':'Underweight'}
        tmp_l = []
        s = BeautifulSoup(self.page_str)
        divs=s.findAll('div', {'class':"stocks-popover"})
        
        for div in divs:
            ticker = div.find('span', {'class':"stock-code"}).text.replace('Stock Code:','').strip()
            if 'HK' in ticker:
                ticker = ticker.replace('HK','').replace('.', '')
            else:
                self.res_df=pd.DataFrame()
                return
            
            recommendation = rec_map[div.find('span', {'class':"status"}).text]
            tgt_price = div.find('span', {'class':"target-price"}).text.replace('Target Price:','').replace('HK','').replace('$','').strip()
            time_per = div.find('span', {'class':"time-estimate"}).text.replace('Time:', '').strip()
            
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
    
    try:
        opts, args = parse()
        obj = Dbs(opts.stdout)
        obj.run_all(opts.db, opts.forceall)
    finally:
        if opts.email: obj.email()       
            

        

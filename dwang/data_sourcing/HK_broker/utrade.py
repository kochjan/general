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

class Utrade(WebScraper):

    def __gen_list__(self):

        base_url = 'http://www.utrade.com.hk/tc/research/technical-quantitative-analysis?page='
        return [base_url+str(i) for i in xrange(1,26)]
        

    def parse_page(self):

        tmp_l = []
        s = BeautifulSoup(self.page_str)
        trs=s.findAll('tr')
        for tr in trs:
            text = tr.text.strip()
            datadate = re.search('(\d+-\d+-\d+) ', text)
            if datadate:
                datadate = dt.datetime.strptime(datadate.groups()[0], '%Y-%m-%d')
                ticker = re.search(u'[\(\uff08](.*)[\)\uff09]', text)
                #import pdb;pdb.set_trace()
                if ticker:
                    ticker = filter(str.isdigit, str(ticker.groups()[0].strip()))
                if ticker:
                    recommendation = text.split('-')[-1]
                    tmp_l.append([datadate, ticker, 'recommendation', recommendation])
                else:
                    #import pdb;pdb.set_trace()
                    self.prt(text)
            else:
                #import pdb;pdb.set_trace()
                self.prt(text)
        
        self.res_df = pd.DataFrame(tmp_l, columns=['datadate', 'ticker', 'category', 'value'])
        self.res_df['source'] = 'utrade'
        self.res_df['last_modification'] = dt.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        self.res_df['sm_localid'] = self.res_df['ticker'].apply(lambda x: 'HK'+str(int(x)))
        self.res_df = match_barrid(self.res_df, bar_localid_df)[['datadate','barrid', 'category', 'value', 'ticker', 'source', 'last_modification']]
        self.res_df = self.res_df.drop_duplicates()
        self.prt(self.res_df)

if __name__ == '__main__':
    
    try:
        opts, args = parse()
        obj = Utrade(opts.stdout)
        obj.run_all(opts.db, opts.forceall)
    finally:
        if opts.email: obj.email()        
            

        

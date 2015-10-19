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

class Abci(WebScraper):

    def __gen_list__(self):

        return ['http://sec.abci.com.hk/chi/research/research.asp']
        

    def parse_page(self):

        tmp_l = []
        s = BeautifulSoup(self.page_str.decode('Big5', 'ignore'))
        trs = s.findAll('tr', {'bgcolor':"#EEF7EE"})
        for tr in trs:
            try:
                #import pdb; pdb.set_trace()
                ticker, value, datadate = re.search('(\d+).* - (.*?)(\d+/\d+/\d+)', tr.text).groups()
                datadate = dt.datetime.strptime(datadate, '%m/%d/%Y')
                tmp_l.append([datadate, ticker, 'recommendation', value])
            except Exception, e:
                print e
        
        self.res_df = pd.DataFrame(tmp_l, columns=['datadate', 'ticker', 'category', 'value'])
        self.res_df['source'] = self.__class__.__name__.lower()
        self.res_df['last_modification'] = dt.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        self.res_df['sm_localid'] = self.res_df['ticker'].apply(lambda x: 'HK'+str(int(x)))

        self.res_df = match_barrid(self.res_df, bar_localid_df)[['datadate','barrid', 'category', 'value', 'ticker', 'source', 'last_modification']]
        self.res_df = self.res_df.drop_duplicates()
        self.prt(self.res_df)

if __name__ == '__main__':
        
    try:
        opts, args = parse()
        obj = Abci(opts.stdout)
        obj.run_all(opts.db, opts.forceall)
    finally:
        if not opts.stdout:
            obj.email()
                    

        

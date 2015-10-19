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

class Publicsec(WebScraper):

    def __gen_list__(self):

        return ['http://www.publicsec.com.hk/Commentary.aspx?Language=Chi']
        

    def parse_page(self):

        tmp_l = []
        s = BeautifulSoup(self.page_str)
        ticker = re.search('>[^\d]+(\d+)[^\d]+<', str(s.findAll('span')[0])).groups()[0]
        tbl = s.find('table', {'class':'red_boxbg'})
        datadate = re.search('([\d]{4}-[\d]{2}-[\d]{2})', str(tbl)).groups()[0]
        trs = tbl.findAll('tr')
        
        tds = trs[2].findAll('td')
        cat1 = tds[0].text
        val1 = tds[1].text
        cat2 = tds[2].text
        val2 = tds[3].text
        
        tds = trs[3].findAll('td')
        cat3 = tds[0].text
        val3 = tds[1].text
        cat4 = tds[2].text
        val4 = tds[3].text

        tmp_l = [[datadate, ticker, cat1, val1], [datadate, ticker, cat2, val2], [datadate, ticker, cat3, val3], [datadate, ticker, cat4, val4]]
        
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
        obj = Publicsec(opts.stdout)
        obj.run_all(opts.db, opts.forceall)
    finally:
        if opts.email: obj.email()
                    

        

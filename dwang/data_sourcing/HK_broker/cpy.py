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

class Cpy(WebScraper):

    def __gen_list__(self):

        return ['http://www.cpy.com.hk/hk/ideas.htm']
        

    def parse_page(self):

        tmp_l = []
        s = BeautifulSoup(self.page_str)
        tbl = s.find('table', {'class':'tpTable'})
        tmp_l = re.findall(""">(\d+/\d+/\d+)<[^\d]+(\d+).*\n.*>(.+)<""", str(tbl))
        
        self.res_df = pd.DataFrame(tmp_l, columns=['datadate', 'ticker', 'value'])
        self.res_df['datadate'] = self.res_df['datadate'].apply(lambda x: dt.datetime.strptime(x, '%d/%m/%Y').strftime('%Y-%m-%d'))
        self.res_df['value'] = self.res_df['value'].apply(lambda x: x.decode('utf-8'))
        self.res_df['category'] = 'recommendation'
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
        obj = Cpy(opts.stdout)
        obj.run_all(opts.db, opts.forceall)
    finally:
        if not opts.stdout:
            obj.email()
                    

        

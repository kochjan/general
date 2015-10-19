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

class KGI(WebScraper):

    def __gen_list__(self):

        return ['http://www.kgieworld.com/ExpertAnalysis/RecommendedStocks.aspx?sc_lang=zh-TW']        

    ## def run_all(self, DB=False):

##         for url in self.list:
##             self.run(url)#self.res_df should be populated
##             #if needed, do aggregation
##             if DB:
##                 self.upload_to_DB()#or, upload_to_DB
            

##         #if needed, upload aggregated results to DB

    def parse_page(self):

        s = BeautifulSoup(self.page_str)
        trs = s.find_all('tr')
        trs = [tr for tr in trs if (u'\u5efa\u8b70' in tr.text) and (r'<tbody>' not in tr.decode())][2:]
        tmp_list = []
        for tri in trs:
            ticker = tri.find('a').text.encode('utf-8')
            tmp = tri.findAll('span')
            datadate = tmp[0].text.encode('utf-8')
            category, value = tmp[-1].text.replace('\n', ',').replace('\r','').replace(' ','').split(',')[1:]
            tmp_list.append([datadate, category, value, ticker])

            tmp = [i for i in tri.text.replace('\n', ',').replace('\r','').replace(' ','').split(',') if i]
            category = tmp[-4]
            value = re.search('\$([\d.]+)', tmp[-3].encode('utf-8')).groups()[0]
            tmp_list.append([datadate, category, value, ticker])
            category = tmp[-2]
            value = re.search('\$([\d.]+)', tmp[-1].encode('utf-8')).groups()[0]
            tmp_list.append([datadate, category, value, ticker])
            #import pdb; pdb.set_trace()

        self.res_df = pd.DataFrame(tmp_list, columns=['datadate', 'category', 'value', 'ticker'])
        self.res_df['source'] = 'KGI'
        self.res_df['last_modification'] = dt.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        self.res_df['sm_localid'] = self.res_df['ticker'].apply(lambda x: 'HK'+str(int(x)))
        #self.res_df = pd.merge(self.res_df, bar_localid_df, left_on='sm_localid', right_on='localid', how='left')[['datadate','barrid', 'category', 'value', 'ticker', 'source', 'last_modification']]
        self.res_df = match_barrid(self.res_df, bar_localid_df)[['datadate','barrid', 'category', 'value', 'ticker', 'source', 'last_modification']]
        self.res_df = self.res_df.drop_duplicates()

if __name__ == '__main__':

        try:
            opts, args = parse()
            obj = KGI(opts.stdout)
            obj.run_all(opts.db, opts.forceall)
        finally:
            if opts.email:
                obj.email()
            

        

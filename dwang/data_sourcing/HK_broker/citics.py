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

class Citics(WebScraper):

    def __gen_list__(self):

        tmp_list=[]
        base_url = 'http://simchigw.infocastfn.com/gate/gb/iportal.infocastfn.com/citiccapital_portal/res/exp.asp?LangId=3&LStkCode=&LStkName=&LECBroker=&Lpage='
        for i in xrange(1,31):
            tmp_list.append(base_url+str(i))
        return tmp_list
        

    def parse_page(self):

        tmp_l = []
        s = BeautifulSoup(self.page_str)
        tbls=s.findAll('table', {'bgcolor':'gray'})
        for tbl in tbls:
            tds = tbl.find('td').findAll('td')
            ticker = re.search('\((\d+)\)', tds[2].text).groups()[0]
            recommendation = tds[3].text
            rec_price = tds[4].text.split('$')[-1]
            tgt_price = tds[6].text.split('$')[-1]
            datadate = re.search('>(\d+\/\d+\/\d+)<', str(tds[7])).groups()[0]
            datadate = dt.datetime.strptime(datadate, '%m/%d/%Y')
            tmp_l.append([datadate, ticker, 'recommendation', recommendation])
            tmp_l.append([datadate, ticker, 'recommend price', rec_price])
            tmp_l.append([datadate, ticker, 'target price', tgt_price])
        
        
        self.res_df = pd.DataFrame(tmp_l, columns=['datadate', 'ticker', 'category', 'value'])
        self.res_df['source'] = 'citics'
        self.res_df['last_modification'] = dt.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        self.res_df['sm_localid'] = self.res_df['ticker'].apply(lambda x: 'HK'+str(int(x)))

        #self.res_df = pd.merge(self.res_df, bar_localid_df, left_on='sm_localid', right_on='localid', how='left')[['datadate','barrid', 'category', 'value', 'ticker', 'source', 'last_modification']]
        self.res_df = match_barrid(self.res_df, bar_localid_df)[['datadate','barrid', 'category', 'value', 'ticker', 'source', 'last_modification']]
        self.res_df = self.res_df.drop_duplicates()
        self.prt(self.res_df)

if __name__ == '__main__':
        
    try:
        opts, args = parse()
        obj = Citics(opts.stdout)
        obj.run_all(opts.db, opts.forceall)
    finally:
        if opts.email: obj.email()
                    

        

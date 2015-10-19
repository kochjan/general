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

class FFG(WebScraper):

    def __gen_list__(self):

        dl = pd.DateRange(dt.datetime(2015, 8, 1), dt.datetime.today(), offset=pd.datetools.bday).tolist()
        dl.reverse()
        return dl
        
    def run_all(self, DB=False, force_all=False):

        base_url = 'http://www2.ffg.com.hk/en/newphp/research_daily_video.php?dailyRCDate='
        url_wrong_l=[]
        for url in self.list:
            self.prt('-'*77)
            datadate = dt.datetime.strftime(url, '%Y-%m-%d')
            self.prt(base_url+datadate)
            try:
                self.run(base_url+datadate)#self.res_df should be populated
            #if needed, do aggregation
                self.res_df['datadate'] = datadate
                self.res_df = match_barrid(self.res_df, bar_localid_df)[['barrid', 'category', 'value', 'ticker', 'source', 'last_modification']]
                self.res_df = self.res_df.drop_duplicates()
                self.prt(self.res_df)
                
                if DB:
                    pk_e = self.upload_to_DB()#or, upload_to_DB
                    if (not force_all) and pk_e:#primary key vialation was met, it means the data are old, we don't need to scrape more pages
                        self.prt("New data have been updated. Stop now.")
                        break
            
            except Exception,e:
                self.prt(e)
                url_wrong_l.append(url)

        self.prt('error urls:')
        self.prt(url_wrong_l)
                
        #if needed, upload aggregated results to DB


    def parse_page(self):
        
        s = BeautifulSoup(self.page_str)
        
        tds = s.findAll('td', {'class':"table_content"})
        tmp_l=[]
        for td in tds:
            if u'\u80a1\u4efd\u63a8\u4ecb' in td:
                rec_blk = re.search(u'\u80a1\u4efd\u63a8\u4ecb([\w\W]*)\u6bcf\u65e5\u4e00\u6cbd[\w\W]*?\((\d+?)\)', td.text)
                buy = rec_blk.groups()[0]
                buy_l = [i for i in buy.replace('\r', '').split('\n') if i]
                buy_l = np.reshape(buy_l, (-1,3)).tolist()
                for i in buy_l:
                    ticker = re.search(u'\((\d+)\)', i[0]).groups()[0]

                    prices = [k.strip() for k in re.findall('(\d+)', i[2])]
                    ctgs = [k.strip() for k in re.findall(u'(\W+)[\uff1a:]', i[2])]
                    price_df = pd.DataFrame(zip(ctgs, prices), columns=['category', 'value'])
                    price_df['ticker'] = ticker
                    
                tmp_l = price_df.values.tolist()
                short_ticker = rec_blk.groups()[-1]
                tmp_l.append([u'\u6bcf\u65e5\u4e00\u6cbd', 'sell', short_ticker])

        if tmp_l:
            #import pdb; pdb.set_trace()
            self.res_df = pd.DataFrame(tmp_l, columns=['category', 'value', 'ticker'])
            self.res_df['source'] = self.__class__.__name__.lower()
            self.res_df['last_modification'] = dt.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
            self.res_df['sm_localid'] = self.res_df['ticker'].apply(lambda x: 'HK'+str(int(x)))

            #self.res_df = pd.merge(self.res_df, bar_localid_df, left_on='sm_localid', right_on='localid', how='left')[['datadate','barrid', 'category', 'value', 'ticker', 'source', 'last_modification']]
            

if __name__ == '__main__':

    try:
        opts, args = parse()
        obj = FFG(opts.stdout)
        obj.run_all(opts.db, opts.forceall)
    finally:
        if not opts.stdout:
            obj.email()
        
            

        

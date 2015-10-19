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

class Poems1(WebScraper):

    def __gen_list__(self):

        base_url = 'http://www.poems.com.hk/en-us/research-and-analysis/louis-talk/?pagenum='
        s = requests.get(base_url+'1').content
        total_p = int(re.search('\xe5\x85\xb1(\d+)\xe9\xa0\x81', s).groups()[0])
        return [base_url+str(i) for i in xrange(1, total_p+1)]

##     def run_all(self, DB=False, force_all=False):

##         for url in self.list:
##             print url
##             self.run(url)#self.res_df should be populated
##             #if needed, do aggregation
##             if DB:
##                 pk_e = self.upload_to_DB()#or, upload_to_DB
##                 if (not force_all) and pk_e:#primary key vialation was met, it means the data are old, we don't need to scrape more pages
##                     print "New data have been updated. Stop now."
##                     break
            

##         #if needed, upload aggregated results to DB

    def parse_page(self):
        #import pdb; pdb.set_trace()
        tbls = re.findall(r'<td class="greenfont" colspan="3">([\d]*-[\d]*-[\d]*)[\w\W]*?<td><span class="style2">([\w\W]*?)<td width="200"', self.page_str)
        ## s = BeautifulSoup(self.page_str)
##         tbls = s.find_all('table')
        tmp_list = []
        for text in tbls:
            try:
                #if u'\u5167\u5bb9\u91cd\u9ede' in text:
                datadate = text[0]#str(re.search('([\d]*-[\d]*-[\d]*)', tbl.find('td', {'class':'greenfont'}).text).groups()[0])
                #category, ticker = re.search(u""",.*,(.*)[:\uff1a].*[\(\uff08](.+)[\)\uff09]""", text).groups()
                tmp = re.search(u"""[\(\uff08](.+)[\)\uff09]""", text[1].decode('utf-8'))
                if tmp:
                    ticker = tmp.groups()[0]
                else:
                    tmp = re.findall('<(.*?)>', text[1])[-3]
                    if not tmp.isdigit():
                        tmp = tmp[:-3]
                    ticker = tmp
                ticker = str(int(ticker))
                tmp_list.append([datadate, ticker])
            except Exception, e:
                self.prt((text[0], text[1]))
                #import pdb; pdb.set_trace()
                self.prt(e)

        if len(tmp_list)==0:
            self.res_df = pd.DataFrame()
            return

        self.res_df = pd.DataFrame(tmp_list, columns=['datadate', 'ticker'])
        self.res_df['datadate'] = self.res_df['datadate'].apply(lambda x: dt.datetime.strptime(x, '%d-%m-%Y'))#.strftime('%Y-%m-%d'))
        self.res_df['source'] = 'poems1'
        self.res_df['last_modification'] = dt.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        self.res_df['sm_localid'] = self.res_df['ticker'].apply(lambda x: 'HK'+str(int(x)))
        self.res_df['category'] = u'\u5fc3\u6c34\u63a8\u4ecb'
        self.res_df['value'] = 'buy'
        #self.res_df = pd.merge(self.res_df, bar_localid_df, left_on='sm_localid', right_on='localid', how='left')[['datadate','barrid', 'category', 'value', 'ticker', 'source', 'last_modification']]
        self.res_df = match_barrid(self.res_df, bar_localid_df)[['datadate','barrid', 'category', 'value', 'ticker', 'source', 'last_modification']]
        self.res_df = self.res_df.drop_duplicates()
        self.prt(self.res_df)

if __name__ == '__main__':
    
        ## obj = Poems1()
##         obj.run_all(True, force_all=False)

        try:
            opts, args = parse()
            obj = Poems1(opts.stdout)
            obj.run_all(opts.db, opts.forceall)
        finally:
            if opts.email: obj.email()
        
            

        

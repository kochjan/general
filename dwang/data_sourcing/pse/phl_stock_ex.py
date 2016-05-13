#!/opt/python2.7/bin/python2.7

import pandas as pd
import requests
import nipun.dbc as dbc
import re
import datetime as dt
import dateutil.parser as dparser
import os
import nipun.sql_io as sql_io
import time

dbo = dbc.db(connect='qai')
url = "http://www.pse.com.ph/stockMarket/marketInfo-marketActivity-marketReports.html"
TABLE = "dwang..phl_foreign_flow"

def fn_generater():
    """
    generate the interal file id for today's download
    """

    sql_max_date = """select top 1 datadate, file_name from %(table)s order by datadate DESC, file_name DESC"""%{'table':TABLE}
    df = dbo.query(sql_max_date, df=True)
    #import pdb; pdb.set_trace()
    if df and len(df)>0:
        datadate_last = df['datadate'].values[0]
        year = (datadate_last + dt.timedelta(1)).year
        fn_last = df['file_name'].values[0]
        fn_l = list(re.findall('(^.*?)(\d{4})(\d*)', fn_last)[0])
        fn_l[1] = str(year)
        fn_l[-1] = str(int(fn_l[-1])+1)
        fn = ''.join(fn_l)
        return fn
    else:
        return "PSE_DQTRT20152967"#DW: this is the file of Mar 09, 2016
    #2967 is a continuous and increasing int (in most cases, but not all), but 2015 is the year
    #PSE_DQTRT20152985 is the last file in 2015
    #PSE_DQTRT20162986 is the first file in 2016

def download_report(fn):
    """
    download the report pdf file
    """

    r = requests.post(url, data={'ajax':'true', 'method':'downloadMarketReports', 'ids':'["%s"]'%fn})
    #import pdb; pdb.set_trace()
    full_fn = '/home/dwang/shared/%s.pdf'%fn
    with open(full_fn, 'w') as f:
        f.write(r.content)

    return full_fn

def check_validity(row):
    valid = False
    if len(row)>=11:
        for k in row[-9:]:
            if k=='-':
                pass
            else:
                try:
                    float(k.replace('(', '').replace(')', '').replace(',', ''))
                except Exception, e:
                    valid = False
                    return valid
        valid = True
    return valid

def find_barrid(datadate, listed_country='PHL'):
    """
    find barrid to localid map
    """

    sql = """select barrid, localid, datadate from nipun_prod..security_master
    where listed_country='%(country)s' and '%(date)s' between datadate and isnull(stopdate, '2050-01-01')
    order by barrid, datadate"""

    df = dbo.query(sql%{'date':datadate.strftime('%Y-%m-%d'), 'country':listed_country}, df=True)
    df = df.drop_duplicates(cols=['barrid'], take_last=True)
    df.pop('datadate')
    df['symbol'] = [i[2:] for i in df['localid']]
    df.pop('localid')
    
    return df

def parser(pdf_fn):
    """
    read and parse the pdf file
    """

    cmd_str = 'pdftotext -raw %s -'%(pdf_fn)
    pdf_content = os.popen(cmd_str).readlines()
    if len(pdf_content)==0:
        raise(Exception('empty pdf file %s was downloaded. pls check http://www.pse.com.ph/stockMarket/marketInfo-marketActivity.html?tab=4'%pdf_fn))
    #once this exception is generated, pls check and manually run main('xxx')
    datadate = dparser.parse(pdf_content[2])
                
    flow_l = []
    for i in pdf_content:
        row = i[:-1].split(' ')
        #print row
        #import pdb; pdb.set_trace()
        if check_validity(row):
            symbol = row[-10]
            if row[-1]=='-':
                flow = None
            else:
                flow = float(row[-1].replace('(', '').replace(')', '').replace(',', ''))
                if '(' in row[-1]: flow = -flow
            flow_l.append([symbol, flow])

    flow_df = pd.DataFrame(flow_l, columns=['symbol', 'net_foreign_flow_php'])
    flow_df['datadate'] = datadate

    barrid_df = find_barrid(datadate, listed_country='PHL')

    flow_df = pd.merge(flow_df, barrid_df, on=['symbol'], how='left')
    flow_df['last_modification'] = dt.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    flow_df['file_name'] = pdf_fn.split('/')[-1][:-4]
    
    return flow_df

def main(internal_fn_orig=None):
    """
    for the 1st file of each year, pls manually set the internal_fn_orig and run this function.
    """
    empty_df_count = 0
    while True:
        if internal_fn_orig is None:
            internal_fn = fn_generater()
        else:
            internal_fn = internal_fn_orig
            internal_fn_orig = None
        print "downloading %s"%internal_fn
        pdf_fn = download_report(internal_fn)
        df = parser(pdf_fn)
        time.sleep(10)
        if df and len(df)>0:
            sql_del = """delete from %(table)s where file_name='%(file_name)s'"""%{'table':TABLE, 'file_name':internal_fn}
            dbo.cursor.execute(sql_del)
            dbo.commit()
            
            sql_io.write_frame(df, TABLE, if_exists='append', bulk='off')
            empty_df_count = 0
            datadate = df['datadate'][0]
            if datadate >= dt.datetime.combine(dt.datetime.today(), dt.time()):
                break
        else:
            empty_df_count+=1
            if empty_df_count>=10:#DW: if get empty data for 10 continuous days, then stop
                break

if __name__ == '__main__':
    main()
                

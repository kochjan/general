#!/opt/python2.7/bin/python2.7
import pandas as pd
#from datetime import datetime as dd
import bs4 as bs

import nipun.dbc as db
import nipun.cpa.load_barra as lb
from nipun_task.utility import sql_io

import glob


dbo = db.db(connect='qai')

def getCategory(catName, soup, day):
    '''for getting table of the 4 top categories'''
    cats = soup.find(catName)
    if not cats:
	print "empty"
        return None
    return [[day, catName, c.category.text.strip(), \
        c.sedol.text, c.cusip.text, c.rank.text, c.region.text, c.country.text, c.sector.text] \
        for c in cats.children if type(c) == bs.element.Tag]


def run(date):
    '''read shortcolor file data, and return the filepath base for other processes'''

    filenames = glob.glob("/home/wzhu/shared/bd.msciShort/Nipun*A/??/reports/{:%Y%m%d}.xml".format(date))

    for fn in filenames:
	print fn
	process_xml_file(fn, date)

def process_xml_file(fn, date):
    ymd = '{:%Y%m%d}'.format(date)
    with open(fn, 'r') as fh:
	xmlcontent = fh.read()
    soup2=bs.BeautifulSoup(xmlcontent, 'xml')
   
    contributions = soup2.find('Contributions') 
    if not contributions:
        print "empty contributions"
    else:
        # 1st category is not instrument specific
	res_contr_df = pd.DataFrame([[ymd, 'Contributions', \
	    c.category.text.strip(), c.contribution.text, c.region.text, c.sector.text] \
	    for c in contributions.children if type(c) == bs.element.Tag], \
	    columns=['datadate', 'section', 'category', 'contribution', 'region', 'sector'])
        res_contr_df['broker'] = 'MSDW'

        if len(res_contr_df) > 0:
            sql_io.write_frame(res_contr_df, 'wzhu.dbo.shortcolor_contr', if_exists='append', bulk='off')
        else:
            print "empty contr table"
    

    # next 4 categories are top list of instruments
    TopCoversByMV_l = getCategory('TopCoversByMV', soup2, ymd)
    TopDTC_l = getCategory('TopDTC', soup2, ymd)
    TopShortsByMV_l = getCategory('TopShortsByMV', soup2, ymd)
    TopShortsByPopularity_l = getCategory('TopShortsByPopularity', soup2, ymd)

    all_top = TopCoversByMV_l+TopDTC_l+TopShortsByMV_l+TopShortsByPopularity_l
    if len(all_top) == 0:
        print "empty top table"
	return

    res_tops_df = pd.DataFrame(all_top, \
	columns=['datadate', 'section', 'category', 'sedol', 'cusip', 'value', 'region', 'country', 'sector'])
    res_tops_df['broker'] = 'MSDW'
    res_tops_df['item'] = 'rank'

    sql_str="""select barrid, sedol from nipun_prod..security_master where '%s' between datadate and isnull(stopdate, '2050-01-01')"""%date.strftime('%Y-%m-%d')
    bar_sedol_df = dbo.query(sql_str, df=True)
    res_tops_df = pd.merge(res_tops_df, bar_sedol_df, left_on=['sedol'], right_on=['sedol'], how='left')

    sql_io.write_frame(res_tops_df, 'wzhu.dbo.shortcolor_tops', if_exists='append', bulk='off')
	

        


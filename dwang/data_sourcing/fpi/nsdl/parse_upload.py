#!/opt/python2.7/bin/python2.7

from bs4 import BeautifulSoup
import nipun.sql_io as sql_io
import glob
import pandas as pd
import datetime as dt

fl = sorted(glob.glob('*.xls'))

for fn in fl:
    print fn
    with open(fn, 'r') as f:
        r = f.read()

    s = BeautifulSoup(r)

    tbls = s.findAll('table')

    if len(tbls)==1:

        tbl = tbls[0]
        trs = tbl.findAll('tr')

        if len(trs[0].findAll('td'))==7:
        
            df_list = []
            for tr in trs[1:]:
                if 'total' in str(tr).lower():
                    break

                tds = tr.findAll('td')
                if len(tds)==7:
                    date = dt.datetime.strptime(tds[0].text.strip(), '%d-%b-%Y')
                    de = tds[1].text.strip()
                    gp = float(tds[2].text.replace('(', '-').replace(')',''))
                    gs = float(tds[3].text.replace('(', '-').replace(')',''))
                    ni = float(tds[4].text.replace('(', '-').replace(')',''))
                    nius = float(tds[5].text.replace('(', '-').replace(')',''))
                    conversion = tds[6].text.strip()
                elif len(tds)==5:
                    date = None
                    de = tds[0].text.strip()
                    gp = float(tds[1].text.replace('(', '-').replace(')',''))
                    gs = float(tds[2].text.replace('(', '-').replace(')',''))
                    ni = float(tds[3].text.replace('(', '-').replace(')',''))
                    nius = float(tds[4].text.replace('(', '-').replace(')',''))
                    conversion = None


                df_list.append([date, de, gp, gs, ni, nius, conversion])
            #import pdb; pdb.set_trace()    
            df = pd.DataFrame(df_list, columns=['Reporting_Date', 'Debt_Equity', 'Gross_Purchases_RsCrore', 'Gross_Sales_RsCrore', 'Net_Investment_RsCrore', 'Net_Investment_US_million', 'Conversion_1_USD_TO_INR'])
            df = df.fillna(method='ffill')
            sql_io.write_frame(df, 'dwang..nsdl', if_exists='append', bulk='off')

        elif len(trs[0].findAll('td'))==8:
            #import pdb; pdb.set_trace()
            df_list = []
            for tr in trs[1:]:
                if 'total for' in str(tr).lower():
                    break
                if 'total' in str(tr).lower():
                    continue

                tds = tr.findAll('td')
                if len(tds)==8:
                    date = dt.datetime.strptime(tds[0].text.strip(), '%d-%b-%Y')
                    de = tds[1].text.strip()
                    ir = tds[2].text.strip()
                    gp = float(tds[3].text.replace('(', '-').replace(')',''))
                    gs = float(tds[4].text.replace('(', '-').replace(')',''))
                    ni = float(tds[5].text.replace('(', '-').replace(')',''))
                    nius = float(tds[6].text.replace('(', '-').replace(')',''))
                    conversion = tds[7].text.strip()
                elif len(tds)==5:
                    date = None
                    de = None
                    ir = tds[0].text.strip()
                    gp = float(tds[1].text.replace('(', '-').replace(')',''))
                    gs = float(tds[2].text.replace('(', '-').replace(')',''))
                    ni = float(tds[3].text.replace('(', '-').replace(')',''))
                    nius = float(tds[4].text.replace('(', '-').replace(')',''))
                    conversion = None


                df_list.append([date, de, ir, gp, gs, ni, nius, conversion])
            #import pdb; pdb.set_trace()    
            df = pd.DataFrame(df_list, columns=['Reporting_Date', 'Debt_Equity', 'Investment_Route', 'Gross_Purchases_RsCrore', 'Gross_Sales_RsCrore', 'Net_Investment_RsCrore', 'Net_Investment_US_million', 'Conversion_1_USD_TO_INR'])
            df = df.fillna(method='ffill')
            sql_io.write_frame(df, 'dwang..nsdl', if_exists='append', bulk='off')

    elif len(tbls)==2:

        tbl = tbls[0]
        trs = tbl.findAll('tr')
        #import pdb; pdb.set_trace()
        if len(trs[1].findAll('th'))==8:

            df_list = []
            for tr in trs[2:]:
                
                if 'total for' in str(tr).lower():
                    break
                if 'total' in str(tr).lower():
                    continue

                tds = tr.findAll('td')
                if len(tds)==8:
                    date = dt.datetime.strptime(tds[0].text.strip(), '%d-%b-%Y')
                    de = tds[1].text.strip()
                    ir = tds[2].text.strip()
                    gp = float(tds[3].text.replace('(', '-').replace(')',''))
                    gs = float(tds[4].text.replace('(', '-').replace(')',''))
                    ni = float(tds[5].text.replace('(', '-').replace(')',''))
                    nius = float(tds[6].text.replace('(', '-').replace(')',''))
                    conversion = tds[7].text.strip()
                elif len(tds)==5:
                    date = None
                    de = None
                    ir = tds[0].text.strip()
                    gp = float(tds[1].text.replace('(', '-').replace(')',''))
                    gs = float(tds[2].text.replace('(', '-').replace(')',''))
                    ni = float(tds[3].text.replace('(', '-').replace(')',''))
                    nius = float(tds[4].text.replace('(', '-').replace(')',''))
                    conversion = None
                elif len(tds)==6:
                    date = None
                    de = tds[0].text.strip()
                    ir = tds[1].text.strip()
                    gp = float(tds[2].text.replace('(', '-').replace(')',''))
                    gs = float(tds[3].text.replace('(', '-').replace(')',''))
                    ni = float(tds[4].text.replace('(', '-').replace(')',''))
                    nius = float(tds[5].text.replace('(', '-').replace(')',''))
                    conversion = None


                df_list.append([date, de, ir, gp, gs, ni, nius, conversion])
            #import pdb; pdb.set_trace()    
            df = pd.DataFrame(df_list, columns=['Reporting_Date', 'Debt_Equity', 'Investment_Route', 'Gross_Purchases_RsCrore', 'Gross_Sales_RsCrore', 'Net_Investment_RsCrore', 'Net_Investment_US_million', 'Conversion_1_USD_TO_INR'])
            #import pdb; pdb.set_trace()
            df = df.fillna(method='ffill')
            sql_io.write_frame(df, 'dwang..nsdl', if_exists='append', bulk='off')

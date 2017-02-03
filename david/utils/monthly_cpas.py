import sys
import datetime
import os
import pandas
import glob

rt252 = pandas.np.sqrt(252)

country_list = ['AUS', 'CHX', 'HKG', 'JPN', 'KOR', 'OTHER', 'SGP', 'TWN', 'CHN']

YM = (datetime.datetime.today() - pandas.datetools.MonthEnd()).strftime('%Y%m')
YMD = (datetime.datetime.today() - pandas.datetools.MonthEnd()).strftime('%Y%m')


def get_files(cn=None, lagged=False, barra=False):
    
    repl = {'ym': YM, 'cn': cn, 'ymd': YMD}
    
    if cn and lagged:
        print cn, 'lagged!'
        rts = glob.glob('/production/monthly/{ym}/lagged/npxchnpak/*/*{cn}*portrets.csv'.format(**repl))
    elif lagged:
        print 'lagged!'
        rts = glob.glob('/production/monthly/{ym}/lagged/npxchnpak/*/port_lag30_b*portrets.csv'.format(**repl))
        print rts 
        rts += glob.glob('/production/monthly/{ym}/lagged/npxchnpak/*/port_lag30_alpha*portrets.csv'.format(**repl))
        print rts
    elif cn and not barra:
        print cn, 'only!'
        pth = '/production/monthly/{ym}/country/npxchnpak/*/port_c{cn}_b_*portrets.csv'.format(**repl)
        print pth
        rts = glob.glob(pth)
        pth = '/production/monthly/{ym}/country/npxchnpak/*/port_c{cn}_alpha*portrets.csv'.format(**repl)
        rts += glob.glob(pth)

    elif barra:
        print cn, 'barra!'
        repl['ym'] = YM 
        rts = glob.glob('/production/monthly/{ym}/barra/npxchnpak/*/{cn}_*portrets.csv'.format(**repl))
   
    else:
        rts = glob.glob('/production/monthly/{ym}/npxchnpak/*/*portrets.csv'.format(**repl))
 
    tmp = []
    print rts
    for fn in rts:
      tdata = pandas.read_csv(fn, index_col=0, parse_dates=True)
      tdata = tdata[['RET1']].rename(columns={'RET1':'_'.join(os.path.basename(fn).split('_')[1:-3])})
      if len(tmp) == 0:
       tmp = tdata
      else:
       tmp = pandas.merge(tmp, tdata, left_index=True, right_index=True, how='outer')

    if len(tmp) == 0:
        return tmp 
    cols = tmp.columns
    if cn: 
        rename = [x.replace('c{cn}_'.format(cn=cn), '').replace('lag30_', '') for x in cols]
    else: 
        rename = [x.replace('prod_', '').replace('lag30_', '') for x in cols]
   
    rename = dict(zip(cols, rename))
    tmp = tmp.rename(columns=rename)
    return tmp

def run_mth(date, tmp):

    incepdate = datetime.datetime(2012, 5, 7)
    ttmdate = date - pandas.datetools.MonthEnd()*12
    mthdate = date - pandas.datetools.MonthEnd()
    ytd_date = date - pandas.datetools.YearEnd()

    ttm = tmp.ix[ttmdate:date].copy()  
    ttm = ttm.mean() * rt252 / ttm.std()
    ttm = pandas.DataFrame(ttm, columns=['ttm'])
    ytd = tmp.ix[ytd_date:date].copy()
    ytd = ytd.mean() * rt252 / ytd.std()
    ytd = pandas.DataFrame(ytd, columns=['ytd'])
    mth = tmp.ix[mthdate:date].copy()
    mth = mth.mean() * rt252 / mth.std()
    mth = pandas.DataFrame(mth, columns=['month'])

    incep = tmp.ix[incepdate:date].copy()
    incep = incep.mean() * rt252 / incep.std()
    incep = pandas.DataFrame(incep, columns=['since_inception'])
    
    data = pandas.merge(mth, ttm, left_index=True, right_index=True)#, suffixes=('', '_ttm'))
    data = pandas.merge(data, ytd, left_index=True, right_index=True)
    data = pandas.merge(data, incep, left_index=True, right_index=True)
    return data.sort()

def test(date):
    
    country_lagged = pandas.DataFrame()
    country_only = pandas.DataFrame()
    all_lagged = pandas.DataFrame()
    barra_all = pandas.DataFrame()

    import nipun.utils as nu
    xls_clag = nu.df2excel()
    xls_cn = nu.df2excel()
    barra = nu.df2excel()

    cols = ['month', 'ttm', 'ytd', 'since_inception']    

    for country in country_list:
        clag = get_files(country, lagged=True)
        if len(clag) > 0:
            clag = run_mth(date, clag)
            clag['country'] = country
            print clag
            xls_clag.create_sheet_with_data(country, clag[cols])

            clag = clag.reset_index()
            country_lagged = country_lagged.append(clag, ignore_index=True)

        else:
            print 'NO lag data for', country
    
        c_nolag = get_files(country, lagged=False)
        if len(c_nolag) > 0:
            c_nolag = run_mth(date, c_nolag)
            c_nolag['country'] = country
            xls_cn.create_sheet_with_data(country, c_nolag[cols])

            c_nolag = c_nolag.reset_index()
            country_only = country_only.append(c_nolag, ignore_index=True)
        else:
            print 'NO data for ', country


        cbarra = get_files(country, lagged=False, barra=True)
        if len(cbarra) > 0:
            cbarra = run_mth(date, cbarra)
            cbarra['country'] = country
            barra.create_sheet_with_data(country, cbarra[cols])

            cbarra = cbarra.reset_index()
        else:
            print 'NO data for ', country

        
     
    lagged = get_files(None, lagged=True)
    lagged = run_mth(date, lagged)

    nolag = get_files(None, False)
#    print nolag
    nolag = run_mth(date, nolag)

    overall = nu.df2excel()
    overall.create_sheet_with_data('overall', nolag[cols])
    overall.create_sheet_with_data('lagged', lagged[cols])


    OUTDIR = '/production/monthly/%s/' % YM

    xls_clag.save('%s/country_lag_%s.xls' % (OUTDIR, YM))
    xls_cn.save('%s/country_%s.xls' % (OUTDIR, YM))
    barra.save('%s/barra_%s.xls' % (OUTDIR, YM))
    overall.save('%s/monthly_irs_%s.xls' % (OUTDIR, YM))

    return

if __name__ == '__main__':
    date = datetime.datetime.today() - pandas.datetools.MonthEnd()
    test(date)

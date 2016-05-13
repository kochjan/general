##THIS IS SIGNAL CODE FOR CROSS HOLDING

import pandas
import nipun.dbc as db
from dateutil.relativedelta import relativedelta
import datetime
from collections import Counter
import numpy as np
import nipun.cpa.load_barra as lb
import nipun.volume as nv
import nipun.utils as nu
import nipun as n
from nipun.utils import write_alpha_files, write_debug_files
import nipun.timing_alphas as ta

PRODUCTION = 0
UNIVERSE = 'npxchnpak'

dbo = db.db(connect='qai')

min_threshold = 10.0 / 10000.0 # 10bps
#min_threshold = 0.0

HOLDINGS_TABLE = 'dsargent..owner_holdings_all'

def pair(as_of_date):

    univ = lb.load_production_universe('npxchnpak', as_of_date)
    barrids = univ.index.tolist()

    sql = ''' 
    select sm.barrid parent_barrid,  sm.name parent_name, oh.* from dsargent..owner_holdings_all oh
	join qai..ownsecmap os on oh.ownercode=os.ownercode
	join nipun_prod..security_master sm on os.securitycode=sm.own_id
	and oh.qtrdate between sm.datadate and isnull(sm.stopdate, '20500101')
	where qtrdate <= '%(max_date)s' and qtrdate >= '%(min_date)s' and sharesheld>=0 and sharesout>=0	
	and sharesheld/sharesout > %(min_threshold)s
    ''' 

    repl = {
            'min_threshold': min_threshold, \
            'max_date': (as_of_date - pandas.datetools.MonthEnd()*3 +datetime.timedelta(1)).strftime('%Y%m%d'), \
            'min_date': (as_of_date - pandas.datetools.MonthEnd()*9 +datetime.timedelta(1)).strftime('%Y%m%d')
            }
    
    ##REPORT DATE MAY BE BACKFILLED, SO NOT USED IN BACKTEST
    ##TO BE CONSERVATIVE, TAKE 3 MON LAG FOR DATADATE

    data = dbo.query(sql % repl, df=True)
    print as_of_date
    print (as_of_date - pandas.datetools.MonthEnd()*3 +datetime.timedelta(1)).strftime('%Y%m%d')
    print (as_of_date - pandas.datetools.MonthEnd()*9 +datetime.timedelta(1)).strftime('%Y%m%d')

    ##PARENT COMPANY NEED TO BE IN UNIVERSE; HOLDINGS CAN BE OUT OF UNIVERSE
    data = data[data['parent_barrid'].isin(barrids)]

    ##When thre are multiple reports on the same qtrdate for the same barrid, keep the most recent one
    data = data.sort(['parent_barrid','barrid','qtrdate','ownercode','reportdate'])
    data = data.drop_duplicates(["parent_barrid","barrid","qtrdate","ownercode"], take_last=True)

    ##When thre are more than one qtrdate, keep the most recent one
    data = data.sort(['parent_barrid','barrid','ownercode','qtrdate'])
    data = data.drop_duplicates(["parent_barrid","barrid","ownercode"], take_last=True)

    cols = ['valueheld', 'valuechg', 'shareschg', 'sharesheld', 'sharesout']
    data[cols] = data[cols].astype(float)   

    ##sum up multiple holdings of the same company by different ownercode of same parent_barrid
    cols = ['parent_barrid','barrid','sharesheld','valueheld']

    sum = data[cols].groupby(['parent_barrid','barrid']).sum()

    sum = sum.rename(columns={'sharesheld':'sharesheld_sum','valueheld':'valueheld_sum'})
    data = pandas.merge(data,sum, how='inner',left_on=['parent_barrid','barrid'],right_index=True)
  
    #data['shares_pct_of_holding'] = data['sharesheld'] /  data['sharesout']

    ##MERGE IN BOOK EQUITY FROM WS
    be = n.getWSCalc(as_of_date, items=['common_equity_ex_mi_lag0'], production=PRODUCTION)
    ttm_current = be.pick_ttm('current')
    be = be.join(ttm_current)

    idx = be['common_equity_ex_mi_lag0'] <= 0
    be['common_equity_ex_mi_lag0'][idx] = np.nan

    print as_of_date
    print "book equity distribution before currency converting"
    print be['common_equity_ex_mi_lag0'].describe()
    print be.head().to_string()
  
    be = be[['common_equity_ex_mi_lag0']]
    data = pandas.merge(data, be, left_on=['parent_barrid'], right_index=True, how='left')

    rskdata = n.loadrsk2('ASE1JPN', 'S', as_of_date, daily=True)

    usdcapt = rskdata[['USD_CAPT','INDNAME','LOC_CAPT']] 
    usdcapt = usdcapt.rename(columns={'USD_CAPT':'USD_CAPT_parent'})
    data = pandas.merge(data, usdcapt, left_on=['parent_barrid'], right_index=True, how='left')

    data['USD_BE'] = data['common_equity_ex_mi_lag0'] * data['USD_CAPT_parent'] / data['LOC_CAPT'] *  1e6 

    usdcapt = rskdata[['USD_CAPT']]
    usdcapt = usdcapt.rename(columns={'USD_CAPT':'USD_CAPT_holding'})
    data = pandas.merge(data, usdcapt, left_on=['barrid'], right_index=True, how='left')

    data['mcap_pct_of_parent'] = data['valueheld_sum'] /  data['USD_CAPT_parent']
    data['mcap_pct_of_holding'] = data['valueheld_sum'] /  data['USD_CAPT_holding']
    data['be_pct_of_parent'] = data['valueheld_sum'] /  data['USD_BE']

    cols = ['USD_CAPT_parent', 'USD_CAPT_holding', 'mcap_pct_of_parent', 'mcap_pct_of_holding','USD_BE', 'be_pct_of_parent']
    data[cols] = data[cols].astype(float)
    print "print distribution of BE_pct_of_parent"
    print data['be_pct_of_parent'].describe().to_string()

    vars = ['mcap_pct_of_holding','mcap_pct_of_parent','be_pct_of_parent']
    for col in vars :
        data[col][data[col] > 1.0] = 1.0

    data = data.sort(['parent_barrid','barrid'])
    data = data.drop_duplicates(["parent_barrid","barrid"], take_last=True)

    ##get omonthly returns
    ##compute PMOM12 PMOM6 pmom3 for every barrid
    retdata = n.monthly_returns(as_of_date, lookback=400, model='GEM2')

    retdata.columns = map(lambda x: x.strip(), retdata.columns.tolist())

    retdata['date'] = map(lambda x: datetime.datetime(int(str(x)[:4]), int(str(x)[4:]), 1)+pandas.datetools.MonthEnd(), retdata['DATADATE'])
    retdata['RET'] += 1

    ##GET PMOM3
    # if as_of_date=20111231, min_date=20111031
    min_date = as_of_date -pandas.datetools.MonthEnd(2)
    print "print pmom3 dates"
    print as_of_date
    print min_date

    retdata3 = retdata[retdata['date'] >= min_date] #(this way we filter out the old months)

    pmom3 = retdata3.groupby('BARRID').apply(lambda x: pandas.np.product(x['RET'])-1)
    pmom3 = pandas.DataFrame(pmom3)
    pmom3.columns = ['pmom3']

    res = pandas.merge(data, pmom3, left_on='barrid', right_index=True)

    ##GET PMOM6
    min_date = as_of_date -pandas.datetools.MonthEnd(5)
    print "print pmom6 dates"
    print as_of_date
    print min_date
    retdata6 = retdata[retdata['date'] >= min_date] #(this way we filter out the old months)

    pmom6 = retdata6.groupby('BARRID').apply(lambda x: pandas.np.product(x['RET'])-1)
    pmom6 = pandas.DataFrame(pmom6)
    pmom6.columns = ['pmom6']

    res = pandas.merge(res, pmom6, left_on='barrid', right_index=True)

    ##GET PMOM12
    min_date = as_of_date -pandas.datetools.MonthEnd(11)
    print "print pmom12 dates"
    print as_of_date
    print min_date
    retdata12 = retdata[retdata['date'] >= min_date] #(this way we filter out the old months)

    pmom12 = retdata12.groupby('BARRID').apply(lambda x: pandas.np.product(x['RET'])-1)
    pmom12 = pandas.DataFrame(pmom12)
    pmom12.columns = ['pmom12']

    res = pandas.merge(res, pmom12, left_on='barrid', right_index=True)

    ##merge in past month return
    retdata = n.monthly_returns(as_of_date, lookback=0, model='GEM2')
    res = pandas.merge(res, retdata, left_on='barrid', right_on='BARRID')

    ##winsorize returns
    idx = res['pmom3'] >= 1
    res['pmom3'][idx] = 1
    idx = (res['pmom3'].notnull()) & (res['pmom3'] < -1)
    res['pmom3'][idx] = -1

    idx = res['pmom6'] >= 1
    res['pmom6'][idx] = 1
    idx = (res['pmom6'].notnull()) & (res['pmom6'] < -1)
    res['pmom6'][idx] = -1

    idx = res['pmom12'] >= 1
    res['pmom12'][idx] = 1
    idx = (res['pmom12'].notnull()) & (res['pmom12'] < -1)
    res['pmom12'][idx] = -1

    idx = res['RET'] >= 1
    res['RET'][idx] = 1
    idx = (res['RET'].notnull()) & (res['RET'] < -1)
    res['RET'][idx] = -1

    ##compute sig = equal-weighted mean of all impulse returns
    grouped = res[['parent_barrid','pmom3','pmom6','pmom12','RET']].groupby('parent_barrid')
    
    holding_pmom3 = pandas.DataFrame(grouped['pmom3'].mean())
    holding_pmom3 = holding_pmom3.rename(columns={'pmom3':'crossholding_pmom3_v1beallhldtype'})
    write_alpha_files(holding_pmom3, 'crossholding_pmom3_v1beallhldtype', as_of_date, production=PRODUCTION)
 
    holding_pmom6 = pandas.DataFrame(grouped['pmom6'].mean())
    holding_pmom6 = holding_pmom6.rename(columns={'pmom6':'crossholding_pmom6_v1beallhldtype'})
    write_alpha_files(holding_pmom6, 'crossholding_pmom6_v1beallhldtype', as_of_date, production=PRODUCTION)

    holding_pmom12 = pandas.DataFrame(grouped['pmom12'].mean())
    holding_pmom12 = holding_pmom12.rename(columns={'pmom12':'crossholding_pmom12_v1beallhldtype'})
    write_alpha_files(holding_pmom12, 'crossholding_pmom12_v1beallhldtype', as_of_date, production=PRODUCTION)

    holding_ret = pandas.DataFrame(grouped['RET'].mean())
    holding_ret = holding_ret.rename(columns={'RET':'crossholding_ret_v1beallhldtype'})
    write_alpha_files(holding_ret, 'crossholding_ret_v1beallhldtype', as_of_date, production=PRODUCTION)

    ##COMPUTE HOLDING PORTFOLIO RETURNS, weighted by VALUEHELD
    
    pf = res[['parent_barrid','valueheld_sum','pmom3','pmom6','pmom12','RET']]
    ## compute weighted sum
    ## the following function sum_weighted_scores returns a couple of columns,
    ## barrid, N, value_weighted, relative_across (value_weighted/N) , relative_within (value_weighted / wts)
    pfret_pmom3 = nu.sum_weighted_scores(pf['parent_barrid'], pf['valueheld_sum'], pf['pmom3'])
    pfret_pmom3['crossholding_pfret_pmom3_v1beallhldtype'] = pfret_pmom3['relative_within']

    write_alpha_files(pfret_pmom3['crossholding_pfret_pmom3_v1beallhldtype'], 'crossholding_pfret_pmom3_v1beallhldtype', as_of_date, production=PRODUCTION)

    pfret_pmom6 = nu.sum_weighted_scores(pf['parent_barrid'], pf['valueheld_sum'], pf['pmom6'])
    pfret_pmom6['crossholding_pfret_pmom6_v1beallhldtype'] = pfret_pmom6['relative_within']

    write_alpha_files(pfret_pmom6['crossholding_pfret_pmom6_v1beallhldtype'], 'crossholding_pfret_pmom6_v1beallhldtype', as_of_date, production=PRODUCTION)

    pfret_pmom12 = nu.sum_weighted_scores(pf['parent_barrid'], pf['valueheld_sum'], pf['pmom12'])
    pfret_pmom12['crossholding_pfret_pmom12_v1beallhldtype'] = pfret_pmom12['relative_within']

    write_alpha_files(pfret_pmom12['crossholding_pfret_pmom12_v1beallhldtype'], 'crossholding_pfret_pmom12_v1beallhldtype', as_of_date, production=PRODUCTION)

    pfret_ret = nu.sum_weighted_scores(pf['parent_barrid'], pf['valueheld_sum'], pf['RET'])
    pfret_ret['crossholding_pfret_ret_v1beallhldtype'] = pfret_ret['relative_within']

    write_alpha_files(pfret_ret['crossholding_pfret_ret_v1beallhldtype'], 'crossholding_pfret_ret_v1beallhldtype', as_of_date, production=PRODUCTION)

    ##compute weighted average returns as impulse
    res = res.sort(['parent_barrid','barrid'])

    res['wgtpmom3'] = np.nan
    res['wgtpmom6'] = np.nan
    res['wgtpmom12'] = np.nan
    res['wgtret'] = np.nan

    for c in set(res['parent_barrid']):
        idx = res['parent_barrid'] == c
        res['wgtpmom3'][idx] = res['be_pct_of_parent'][idx] * res['pmom3'][idx]
        res['wgtpmom6'][idx] = res['be_pct_of_parent'][idx] * res['pmom6'][idx]
        res['wgtpmom12'][idx] = res['be_pct_of_parent'][idx] * res['pmom12'][idx]
        res['wgtret'][idx] = res['be_pct_of_parent'][idx] * res['RET'][idx]

    ##compute sig = weighted SUM of all impulse returns
    grouped = res[['parent_barrid','wgtpmom3','wgtpmom6','wgtpmom12','wgtret']].groupby('parent_barrid')
    holding_pmom3 = pandas.DataFrame(grouped['wgtpmom3'].sum())
    holding_pmom3 = holding_pmom3.rename(columns={'wgtpmom3':'crossholding_wgtpmom3_v1beallhldtype'})
    write_alpha_files(holding_pmom3, 'crossholding_wgtpmom3_v1beallhldtype', as_of_date, production=PRODUCTION)

    crossholding_wgtpmom3_v1beallhldtype_in, crossholding_wgtpmom3_v1beallhldtype_it = ta.timing_alphas(holding_pmom3, 'crossholding_wgtpmom3_v1beallhldtype', as_of_date, UNIVERSE, 'INDUSTRY', production=PRODUCTION)
    #write_alpha_files(crossholding_wgtpmom3_v1beallhldtype_in, 'crossholding_wgtpmom3_v1beallhldtype_in', as_of_date, production=PRODUCTION)

    holding_pmom6 = pandas.DataFrame(grouped['wgtpmom6'].sum())
    holding_pmom6 = holding_pmom6.rename(columns={'wgtpmom6':'crossholding_wgtpmom6_v1beallhldtype'})
    write_alpha_files(holding_pmom6, 'crossholding_wgtpmom6_v1beallhldtype', as_of_date, production=PRODUCTION)

    crossholding_wgtpmom6_v1beallhldtype_in, crossholding_wgtpmom6_v1beallhldtype_it = ta.timing_alphas(holding_pmom6, 'crossholding_wgtpmom6_v1beallhldtype', as_of_date, UNIVERSE, 'INDUSTRY', production=PRODUCTION)
    #write_alpha_files(crossholding_wgtpmom6_v1beallhldtype_in, 'crossholding_wgtpmom6_v1beallhldtype_in', as_of_date, production=PRODUCTION)

    holding_pmom12 = pandas.DataFrame(grouped['wgtpmom12'].sum())
    holding_pmom12 = holding_pmom12.rename(columns={'wgtpmom12':'crossholding_wgtpmom12_v1beallhldtype'})
    write_alpha_files(holding_pmom12, 'crossholding_wgtpmom12_v1beallhldtype', as_of_date, production=PRODUCTION)

    crossholding_wgtpmom12_v1beallhldtype_in, crossholding_wgtpmom12_v1beallhldtype_it = ta.timing_alphas(holding_pmom12, 'crossholding_wgtpmom12_v1beallhldtype', as_of_date, UNIVERSE, 'INDUSTRY', production=PRODUCTION)
    #write_alpha_files(crossholding_wgtpmom12_v1beallhldtype_in, 'crossholding_wgtpmom12_v1beallhldtype_in', as_of_date, production=PRODUCTION)

    holding_ret = pandas.DataFrame(grouped['wgtret'].sum())
    holding_ret = holding_ret.rename(columns={'wgtret':'crossholding_wgtret_v1beallhldtype'})
    write_alpha_files(holding_ret, 'crossholding_wgtret_v1beallhldtype', as_of_date, production=PRODUCTION)
  
    crossholding_wgtret_v1beallhldtype_in, crossholding_wgtret_v1beallhldtype_it = ta.timing_alphas(holding_ret, 'crossholding_wgtret_v1beallhldtype', as_of_date, UNIVERSE, 'INDUSTRY', production=PRODUCTION)
    #write_alpha_files(crossholding_wgtret_v1beallhldtype_in, 'crossholding_wgtret_v1beallhldtype_in', as_of_date, production=PRODUCTION)


    return True


def run(as_of_date):

    results = pair(as_of_date)
    return True







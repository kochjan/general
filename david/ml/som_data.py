import nipun.returns as rets

import pandas
import nipun.cpa.load_barra as lb

import nipun.dbc as dbc
dbo = dbc.db(connect='gce-data')

def run(date):
    print date
    """
    '''
    compute cumulative FlowPct over the past month
    '''
    epfr_sql = '''
        select FlowPct from nipun_prod.epfr_daily_asiaxjpn_flow
        where ReportDate between '{startdate:%Y%m%d}' and '{enddate:%Y%m%d}'
    '''.format(
        startdate=date-pandas.datetools.MonthEnd()-pandas.datetools.BDay(),
        enddate=date-pandas.datetools.BDay()
    )

    query = dbo.query(epfr_sql, df=True)
    if query is None:
        epfr_asia = None
        print 'epfr is None in month of {:%Y%m%d}'.format(date)
    else:
        epfr_asia = query['flowpct'].sum()
    """

    '''
    Use current predicted IR
    '''
    epfr_sql = '''
            select predicted_IR from nipun_prod.flow_predicted_sigma
            where datadate = '{usedate:%Y%m%d}'
        '''.format(usedate=date-pandas.datetools.BDay(2))
    query = dbo.query(epfr_sql)
    if query is None:
        epfr_asia = None
        print 'epfr is None in month of {:%Y%m%d}'.format(date)
    else:
        epfr_asia = query[0][0]

    data = pandas.DataFrame()
    rsk = lb.loadrsk2('ase1jpn', 'S', date)
    country_list = ['HKG', 'TWN', 'KOR', 'JPN', 'AUS', 'SGP', 'OTHER']
    alps = ['sentiment', 'analyst', 'quality', 'iu', 'fmom', 'value']
    for alp in alps:

        try:
            _tmp = lb.loadalp('/research/alphagen/alpha_v5/b_%s__w/b_%s__w_%s.alp' % (alp, alp, date.strftime('%Y%m%d')))
            _tmp.columns = [alp]
        except Exception:
            print 'alpha {} not loaded on {:%Y%m%d}'.format(alp, date)
            # no research alphas after 20160831
            continue

        if len(data) == 0:
            data = _tmp

        else:
            data = pandas.merge(data, _tmp, left_index=True, right_index=True, how='outer')

    forward_rets = rets.monthly_resrets(date+pandas.datetools.MonthEnd(), lookback=0)
    forward_rets = forward_rets[['resid_c', 'BARRID']]
    forward_rets.set_index('BARRID', inplace=True)
    data = pandas.merge(data, forward_rets, left_index=True, right_index=True, how='inner')
    data = data.fillna(0.0)
    data['COUNTRY'] = rsk['COUNTRY']
    ix = data['COUNTRY'] == 'CHX'
    data['COUNTRY'][ix] = 'HKG'

    ix = data['COUNTRY'].isin(country_list)
    data['COUNTRY'][-ix] = 'OTHER'
    data['date'] = date.strftime('%Y%m%d')
    data['epfr'] = epfr_asia
    data.to_csv('som/som.input_data/%s.csv' % date.strftime('%Y%m%d'), index=True, header=True)

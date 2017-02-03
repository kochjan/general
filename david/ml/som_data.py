import nipun.returns as rets

import pandas
import nipun.cpa.load_barra as lb

def run(date):

    data = pandas.DataFrame()
    rsk = lb.loadrsk2('ase1jpn', 'S', date)
    country_list = ['HKG', 'TWN', 'KOR', 'JPN', 'AUS', 'SGP', 'OTHER']
    alps = ['sentiment', 'analyst', 'quality', 'iu', 'fmom', 'value']
    for alp in alps:

        _tmp = lb.loadalp('/research/alphagen/alpha_v5/b_%s__w/b_%s__w_%s.alp' % (alp, alp, date.strftime('%Y%m%d')))
        _tmp.columns = [alp]

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
    data.to_csv('input_data/%s.csv' % date.strftime('%Y%m%d'), index=True, header=True)

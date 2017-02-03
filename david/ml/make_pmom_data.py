import nipun.cpa.winsor as wins
import nipun.returns as ret
import pandas
import datetime

def run(date):

    pred_month = ret.monthly_returns(date+pandas.datetools.MonthEnd(), lookback=0)
    pred_month.set_index('BARRID', inplace=True)

    pred_month2 = ret.monthly_returns(date+pandas.datetools.MonthEnd()*2, lookback=0)
    pred_month2.set_index('BARRID', inplace=True)

    pred_month3 = ret.monthly_returns(date+pandas.datetools.MonthEnd()*3, lookback=0)
    pred_month3.set_index('BARRID', inplace=True)
    pred_month3 = pred_month3.rename(columns={'RET':'RET_f3'})
    
    pdata = pandas.merge(pred_month, pred_month2, left_index=True, right_index=True, suffixes=('_f1', '_f2'), how='left')
    pdata = pandas.merge(pdata, pred_month3, left_index=True, right_index=True, how='left')
    pdata['FRET_F1'] = 1+pdata['RET_f1'].fillna(0)
    pdata['FRET_F2'] = (1+pdata['RET_f1'].fillna(0)) * (1+pdata['RET_f2'].fillna(0))
    pdata['FRET_F3'] = (pdata['FRET_F2'].fillna(0)) * (1+pdata['RET_f3'].fillna(0))
    pdata = pdata[['FRET_F1', 'FRET_F2', 'FRET_F3']]


    monthly_data = ret.monthly_returns(date-pandas.datetools.MonthEnd(), lookback=375)
    daily_data = ret.daily_returns(date, lookback=30)

    udts = sorted(sorted(monthly_data['DATADATE'].unique().tolist())[-13:], reverse=True)
    udaily = sorted(sorted(daily_data['DATADATE'].unique().tolist())[-20:], reverse=True)

    mthly_dates = dict(zip(udts, ['month_t%s' % x for x in range(len(udts))]))
    daily_dates = dict(zip(udaily, ['day_t%s' % x for x in range(len(udaily))]))

    monthly_data['month_idx'] = monthly_data['DATADATE'].apply(lambda x: mthly_dates.get(x))   
    daily_data['daily_idx'] = daily_data['DATADATE'].apply(lambda x: daily_dates.get(x))

    monthly_data = monthly_data[monthly_data['month_idx'].notnull()]
    daily_data = daily_data[daily_data['daily_idx'].notnull()]

    monthly = monthly_data.pivot(index='BARRID', columns='month_idx', values='RET')
    daily = daily_data.pivot(index='BARRID', columns='daily_idx', values='RET')

    data = pandas.merge(monthly, daily, left_index=True, right_index=True, how='inner')
    data = pandas.merge(data, pdata, left_index=True, right_index=True, how='inner')

    ix = [x[2] != 'Q' for x in data.index]
    data = data.ix[ix]

    for col in data:
        data[col] = wins.winsor(data[col].fillna(0))

    data.to_csv('pmom/%s.csv' % date.strftime('%Y%m%d')) 

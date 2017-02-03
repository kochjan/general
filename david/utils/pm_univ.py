
import pandas
import datetime
import nipun.dbc as dbc
import nipun.cpa.load_barra as lb
dbo = dbc.db(connect='qai')


def loadaxioma(date):

    sql = '''
    select * from nipun_prod..axioma_info where
    datadate between '%s' and '%s'
    order by datadate
    ''' % (date-datetime.timedelta(7), date)
    data = dbo.query(sql, df=True)
    data = data.drop_duplicates(['barrid'], take_last=True)
    return data

def run(date):

    if pandas.datetools.isMonthEnd(date):
        return
    udate = date-pandas.datetools.MonthEnd()
    print date, udate
    univ = lb.loaduniv('npx_big', udate)
    univ = univ[['SEDOL', 'NATURAL', 'VALID_UNTIL']]
    univ['VALID_UNTIL'] = univ['VALID_UNTIL'].apply(lambda x: x.strftime('%Y%m%d'))
    univ['NATURAL'] = univ['NATURAL'].astype(int)

    univ.to_csv('/research/data/universe/npx_big/npx_big%s.univ' % date.strftime('%Y%m%d'), sep='|', header=False, index=True)
    

def run1(date):

    jpntop = lb.load_production_universe('jpn400', date)
    univ = lb.load_production_universe('npxchnpak', date)
    univ['cnt'] = map(lambda x: x.upper()[:3], univ.index)

    jpn = univ[univ['cnt'] == 'JPN']
    ix = jpn.index.isin(jpntop.index)
    jpn = jpn.ix[-ix]
    jpn = jpn.reset_index().rename(columns={'BARRID':'barrid'})

    univ = univ[univ['cnt'] != 'JPN']
    
    ax = loadaxioma(date)
    data = pandas.merge(ax, univ, left_on='barrid', right_index=True)
    med = data.groupby('cnt')['adv_20d'].median()
    data = pandas.merge(data, pandas.DataFrame(med, columns=['med_adv']), left_on='cnt', right_index=True)

    print med
    data = data[data['adv_20d'] >  data['med_adv']]
    data = data.append(jpn.reset_index(), ignore_index=True)

    data = data[['barrid', 'SEDOL', 'NATURAL', 'VALID_UNTIL']]
    data['VALID_UNTIL'] = data['VALID_UNTIL'].apply(lambda x: x.strftime('%Y%m%d'))
    data['NATURAL'] = data['NATURAL'].astype(int)
    
    data.to_csv('/research/data/universe/npx_big/npx_big%s.univ' % date.strftime('%Y%m%d'), sep='|', header=False, index=False)

import nipun.cloud.alphas as ca
import nipun.cloud.data as cd
import pandas

VERSION='ml-base-v2'

def run(date):

    dg = lambda x: date - pandas.datetools.BDay()*x
    dates = [dg(x) for x in range(0, 71, 10)]

    data = pandas.DataFrame()
    for dt in dates:
        tmp = cd.read_gce('users', 'dsargent/{v}/{d}.pd'.format(v=VERSION, d=dt.strftime('%Y%m%d'))).reset_index()
        tmp = tmp.rename(columns={'index': 'barrid'})
        data = data.append(tmp, ignore_index=True)
   
    for col in data:
        try:
            data[col] = wins.winsor(data[col])
            data[col] = data[col].fillna(0.0)
        except:
            pass 

    data = data.dropna()

    k = ['cratio_e', 'pmom', 'price_target'] 
    data['super'] = data[k].sum(axis=1)
    data = data.groupby('barrid')['super'].mean().reset_index()
    print data.head()
    ca.write_alpha(data, 'cjs_alpha', 'npxchnpak', date)


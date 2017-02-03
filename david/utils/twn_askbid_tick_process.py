# coding: utf-8
import gzip
import glob

file_regex = 'mark.chan@nipuncapital.com-myFTP1-N60074636-part'


regexes = ['mark.chan@nipuncapital.com-myFTP1-N61089493', 'mark.chan@nipuncapital.com-myFTP1-N61233218', 
           'mark.chan@nipuncapital.com-myFTP1-N61755919', 'mark.chan@nipuncapital.com-myFTP1-N61905511',
           'david.sargent@nipuncapital.com-myFTP1-N61965965']

files = glob.glob('/home/mchan/outputs/'+file_regex+'*.csv.gz')
#for file_regex in regexes:
#    files += glob.glob('/home/mchan/outputs/'+file_regex+'*.csv.gz')

def run_file(f, jobid):

    print "Processing: ", f
    z = gzip.GzipFile(f)

    header = ['ric', 'date' 'time', 'type', 'exctonrbid', 'price', 'vol', 'bidprice', 'bidsize', 'askprice', 'asksize', 'tickdir']
    i = 0

    last_datetick = [None, None]
    last_spread = None

    fh = file('twn_stats/twn_tick_stats_%s.csv' % jobid, 'w')

    summary_vol = {}
    summary_not = {}
    summary_cnt = {}
    for line in z:

        if i == 0:
            i += 1
            continue

        appends = []
        row = line[:-1].split(',')
        if row[3] == 'Trade':

            #twn use 14:3
            #kor use 16:
            if row[2].startswith('14:3'):
                continue

            appends.extend(row[:3])

            if row[0:2] == last_datetick:
                try:
                    row[5] = float(row[5])
                    row[6] = float(row[6]) 
                except:
                    print row
                    continue
                if row[5] > last_spread or row[11] == '^':
                    appends.append('B')
                elif row[5] < last_spread or row[11] == 'v':
                    appends.append('S')
                else:
                    appends.append('X')

                appends.extend(row[5:7])

            else:
                try:
                    row[5] = float(row[5])
                    row[6] = float(row[6]) 
                except:
                    print row

                if row[11] == '^':
                    appends.append('B')
                elif row[11] == 'v':
                    appends.append('S')
                else:
                    appends.append('X')

                appends.extend(row[5:7])
           
            ### make dict
            key = (appends[0], appends[1], appends[3])
            if appends[5] == '':
                continue

            if len(appends) > 3:
                try:
                    summary_vol[key] += appends[4]*1000
                    summary_not[key] += (appends[4]*appends[5]*1000)
                    summary_cnt[key] += 1
                except:
                    summary_vol[key] = appends[4]*1000
                    summary_not[key] = (appends[4]*appends[5]*1000)
                    summary_cnt[key] = 1

        elif row[3] == 'Quote':
            try:
                last_spread = (float(row[7]) + float(row[9])) / 2.0
            except:
                pass

        last_datetick = row[:2]

    for key in summary_vol:
        vol = summary_vol[key]
        notional = summary_not[key]
        cnt = summary_cnt[key]
        fh.write(','.join(map(str, [key[0], key[1], key[2], vol, notional,cnt]))+'\n')

    fh.flush()
    fh.close()
    return 

def do_mapping(data):

    import datetime
    import nipun.dbc as dbc
    dbo = dbc.db(connect='qai')
    sql = '''select distinct datadate, localid, barrid from nipun_prod..security_master
    where barrid is not null and localid is not null
    and listed_country='TWN'
    '''

    idmap = dbo.query(sql, df=True)
    idmap['ticker'] = idmap['localid'].apply(lambda x: x.upper()[2:])

    data = data.copy()
    data['ticker'] = data['ric'].apply(lambda x: x.split('.')[0])

    data['barrid'] = None
    udates = map(lambda x: datetime.datetime.strptime(str(x)[:8], '%Y%m%d'), data['date'].unique().tolist())

    for udt in udates:

        ix = data['date'] == int(udt.strftime('%Y%m%d'))
        idx_map = idmap['datadate'] <= udt
        _tmp = idmap[idx_map].sort('datadate').drop_duplicates(['ticker'], take_last=True)
        _tmp.set_index('ticker', inplace=True)
        _tmp = _tmp['barrid'].to_dict()
        data['barrid'][ix] = data['ticker'][ix].apply(_tmp.get)
        print udt

    return data


if __name__ == '__main__':

    def step1():
        import pp
        jobs = []
        ppserver = pp.Server(ppservers=(), ncpus=12)
        
    #    need_list = file('need_files.csv', 'r').readlines()
    #    need_list = map(lambda x: int(x.replace('\n','').strip()), need_list)
        jobids = 0
        for fn in files:
         #   if jobids in need_list:
         #       pass
         #   else:
         #       jobids+=1
         #       continue
            print fn
            jobs.append(ppserver.submit(run_file, (fn, jobids), (), ('gzip', )))
            jobids+=1
        
        for job in jobs:
            job()

        ppserver.destroy()

    def step2():

        import pandas
        fns = glob.glob('kor_stats/*.csv')

        all_data = pandas.DataFrame()


        def proc_file(fn):
            import pandas
            print fn
            data = pandas.read_csv(fn, names=['ric', 'date', 'time', 'side', 'price', 'volume'])
            data['notional'] = data['price'] * data['volume'] * 1000
            grp = data.groupby(['ric', 'date', 'side'])
            total_notional = pandas.DataFrame(grp['notional'].sum())
            total_volume = pandas.DataFrame(grp['volume'].sum() * 1000)

            _tmp = pandas.merge(total_notional, total_volume, left_index=True, right_index=True)
            _tmp = _tmp.reset_index()
            return _tmp

        import pp
        jobs = []
        ppserver = pp.Server(ppservers=(),ncpus=10)
        jobid=0
        for fn in fns:
            jobs.append(ppserver.submit(proc_file, (fn,), (), ('pandas',)))

        for job in jobs:
            _tmp = job()
            all_data = all_data.append(_tmp, ignore_index=True)

        all_data = all_data.groupby(['ric', 'date', 'side']).sum().reset_index()
        all_data.to_csv('kor_all_stats.csv', index=False, header=True)


    print 'processing ticks'
    step1()



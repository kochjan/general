import re
import pandas
import subprocess
import datetime

MATCH_LEN = 34

def parse_sv(date):

    try:
        fname = '%s-ge.pdf' % date.strftime('%y%m%d')
        proc = subprocess.Popen('pdftotext -raw %s -' % fname, cwd='pdfs/', shell=True, stdout=subprocess.PIPE)
        raw = proc.stdout.read()
    except Exception,e :
        print e
        return

    replace = {'Warehousing & Harbor\nTransportation Services\n': 'Warehousing & Harbor Transport Service ', \
            'Others \xef\xbc\x88excluding\nthe above 33 industries)\n': 'Others'}

    for repl in replace.keys():
        raw = raw.replace(repl, replace[repl])
    raw = raw.split('\n')

    if len(raw) == 0:
        return
    output = []
    for row in raw:
        row = row.replace(',','').replace('%', '')
        cols = re.findall(r'(\d+\.?\d+)', row)
        if len(cols) != 7:
            continue
        sector = re.findall(r'\D+', row)[0].strip()
        cols.insert(0, sector)
        output.append(cols) 
    
    if len(output) != MATCH_LEN:
        return
        raise ValueError('found %s entries, expecting %s' % len(output), MATCH_LEN)

    col_names = ['sector', 'selling_val', 'selling_pct', \
                'shorting_val', 'shorting_pct', \
                'shorting_no_restrict', 'shorting_no_restrict_pct', 'total']

    data = pandas.DataFrame(output, columns=col_names)
    
    pct_cols = ['selling_pct', 'shorting_pct', 'shorting_no_restrict_pct']
    for col in pct_cols:
        data[col] = data[col].astype(float) / 100.0

    data['date'] = date.strftime('%Y%m%d')

    data.to_csv('csv/{:%Y%m%d}-ge.csv'.format(date), index=False, header=False)
    return

for dt in pandas.DateRange(datetime.datetime(2014,7,1), datetime.datetime(2015,7,27)):
    parse_sv(dt)

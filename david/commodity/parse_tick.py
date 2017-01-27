


import gzip
import glob
fns = sorted(glob.glob('*.csv.gz'))

fout = file('tick_data_trades_v2.csv', 'w')
last_known_quote = {}

def parse_file(fn):
    print fn
    f = gzip.open(fn, 'r')
    for line in f:
        ric, date, time, qtype, price, vol, bid_price, bid_size, ask_price, ask_size = line.split(',')
        if qtype == 'Quote':
            
            try:
                last_known_quote[ric][0] = date
                last_known_quote[ric][1] = time
                if bid_price != '':
                    last_known_quote[ric][2] = bid_price
                    last_known_quote[ric][3] = bid_size

                if ask_price != '':
                    last_known_quote[ric][4] = ask_price
                    last_known_quote[ric][5] = ask_size
            except KeyError:
                last_known_quote[ric] = [date, time, bid_price, bid_size, ask_price, ask_size]
    
        elif qtype == 'Trade':
            try:
                quote = last_known_quote[ric][:]
                quote.extend([ric, date, time, price, vol])
                fout.write(','.join(quote).replace('\n', '')+'\n')
                fout.flush()
            except:
                pass

    return

# parse files
for fn in fns:
    parse_file(fn) 

        

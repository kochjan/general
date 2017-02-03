
import nipun.utils as nu
import nipun.cpa.load_barra as lb

def run(date):

    alps = ['MOMENTUM', 'LIQUIDIT', 'VALUE', 'GROWTH', 'SIZE', 'VOLATILI', 'LEVERAGE', 'MOMENTUM']
    rsk = lb.loadrsk2('ase1jpn', 'S', date, daily=True)

    for a in alps:
        nu.write_alpha_files(rsk[a], a.lower(), date)


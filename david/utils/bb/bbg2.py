import datetime
import blpapi
from SimpleXMLRPCServer import SimpleXMLRPCServer

def get_anr(ident):

    ### returns a dictionary of security - > {fld}
    currency = make_generic_bulk(ident, 'CRNCY')
    
    raw = make_bulk_request(ident, 'BEST_ANALYST_RECS_BULK')
    ### this comes back with 3 msgs
    seqs = filter(lambda x: str(x.messageType()) == 'ReferenceDataResponse', raw)
    

    all_data = []
    all_data.append(['security', 'firm_name', 'analyst_name', 'recommendation', 'rating', 'target_price', 'target_horizon', 'rec_date', 'currency'])

    for raw_data in seqs:
        anr_all = raw_data.getElement('securityData')
        n_securities = anr_all.numValues()

        for x in range(n_securities):
    
            secid = anr_all.getValueAsElement(x)
            security = secid.getElementAsString('security')
            try:
                anr = secid.getElement('fieldData').getElement('BEST_ANALYST_RECS_BULK')
            except:
                continue    
            n_recs = anr.numValues()
            for i in range(n_recs):
    
                rec = anr.getValueAsElement(i)
                firm = rec.getElementAsString('Firm Name')
                analyst = rec.getElementAsString('Analyst')
                reco = rec.getElementAsString('Recommendation')
                rating = rec.getElementAsInteger('Rating')
                targ_price = rec.getElementAsFloat('Target Price')
                targ_horiz = rec.getElementAsString('Period')
                recdate = datetime.datetime.combine(rec.getElementAsDatetime('Date'), datetime.time())
                try:
                    sec_crncy = currency[security]['CRNCY']
                except:
                    sec_crncy = 'N/A'
                all_data.append((security, firm, analyst, reco, rating, targ_price, targ_horiz, recdate, sec_crncy))

    return all_data

def make_generic_bulk(ident, field):
    if not isinstance(field, list):
        field = [field]
    raw = make_bulk_request(ident, field)
    seqs = filter(lambda x: str(x.messageType()) == 'ReferenceDataResponse', raw)

    all_sec_data = {}
    for raw_data in seqs:
    
        secdata = raw_data.getElement('securityData')
        n_securities = secdata.numValues()

        for i in range(n_securities):

            _sec = secdata.getValueAsElement(i)
            sec_ident = _sec.getElementAsString('security')
            _fields = _sec.getElement('fieldData')
            all_sec_data[sec_ident] = {}
            for key in field:
                try:
                    all_sec_data[sec_ident][key] = _fields.getElementAsString(key)
                except:
                    all_sec_data[sec_ident][key] = None

    return all_sec_data


def make_bulk_request(ident, field):

    session = blpapi.Session()
    if not session.start():
        return ['Error starting session']

    if not session.openService('//blp/refdata'):
        return ['Error opening ref data connection']
    refservice = session.getService('//blp/refdata')
    req = refservice.createRequest('ReferenceDataRequest')

    if isinstance(ident, list):
        for _id in ident:
            req.append('securities', _id)
        
    else:
        req.append('securities', ident)

    if isinstance(field, list):
        for fld in field:
            req.append('fields', fld)
    else:
        req.append('fields', field)

    session.sendRequest(req)
    result_data = []
    while(True):

        ev = session.nextEvent()
        for msg in ev:
            result_data.append(msg)

        if ev.eventType() == blpapi.Event.RESPONSE:
            break

    
    session.stop()
    return result_data



def run_server():
    server = SimpleXMLRPCServer(('', 8889), allow_none=True)
    server.register_function(get_anr, 'anr_update')
    server.register_function(make_generic_bulk, 'bbg')
    server.serve_forever()

run_server()


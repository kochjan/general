import win32com.client as wc
from SimpleXMLRPCServer import SimpleXMLRPCServer

w = wc.Dispatch('Bloomberg.Data.1')


def bbg_lookup(idlist, fld):

    id_res = w.BLPSubscribe(idlist, fld)
    outputs = []

    for i in xrange(len(idlist)):
        x = [idlist[i]]
        x.extend(id_res[i])

        outputs.append(x)#[idlist[i], id_res[i][0]]) 

    w.Flush()
    
    return outputs


def run_server():

    server = SimpleXMLRPCServer(('', 8888), allow_none=True)
    server.register_function(bbg_lookup, 'bbg')

    server.serve_forever()


run_server()


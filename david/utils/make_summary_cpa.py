
import sys
import pyPdf
import glob

def run(path):
    output = pyPdf.PdfFileWriter()
    fns = glob.glob(path+'/*/*.pdf')
    for fn in fns:
        tmp = pyPdf.PdfFileReader(file(fn, 'rb'))
        output.addPage(tmp.getPage(2))
    fname = './summary.pdf'
    outf = file(fname, 'wb')
    output.write(outf)
    outf.close()
    return
if __name__ == '__main__':
    run(sys.argv[1])

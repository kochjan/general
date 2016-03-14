#!/opt/python2.7/bin/python
#/usr/bin/python
# author: http://sujitpal.blogspot.com/2006/07/python-xml-viewer-for-linux.html
# A simple SAX Parser to view large XML files as a nicely formatted XML
# document tree. Pipe the output through less and move forward and backward
# using [SPACE] and [CTRL-B] respectively. Standard less keyboard commands
# will also work.
#
# mod:
# add unicode capability by removing the 'string' class
# and followed http://stackoverflow.com/questions/857597/setting-the-encoding-for-sax-parser-in-python
import sys
currentStdOut = sys.stdout
currentStdIn = sys.stdin
currentStdErr = sys.stderr

reload(sys)
sys.setdefaultencoding('utf-8')

sys.stdout = currentStdOut
sys.stdin = currentStdIn
sys.stderr = currentStdErr

from xml.sax import make_parser
from xml.sax.handler import ContentHandler

class PrettyPrintingContentHandler(ContentHandler):
    """ Subclass of the SAX ContentHandler to print document tree """

    def __init__(self, indent):
        """ Ctor """
        self.indent = indent
        self.level = 0
        self.chars = ''

    def startElement(self, name, attrs):
        """ Set the level and print opening tag with attributes """
        self.level = self.level + 1
        attrString = ""
        qnames = attrs.getQNames()
        for i in range(0, len(qnames)):
            attrString = attrString + " " + qnames[i] + "=\"" + attrs.getValueByQName(qnames[i]) + "\""
        print self.tab(self.level) + "<" + name.rstrip() + attrString + ">"

    def endElement(self, name):
        """ Print the characters and the closing tag """
        if (len(self.chars.strip()) > 0):
            print self.tab(self.level + 1) + self.chars.rstrip()
        self.chars = ''
        print self.tab(self.level) + "</" + name.rstrip() + ">"
        self.level = self.level - 1

    def characters(self, c):
        """ Accumulate characters, ignore whitespace """
        if (len(c.strip()) > 0):
            self.chars = self.chars + c

    def tab(self, n):
        """ Print the tabstop for the current element """
        tab = ""
        for i in range(1, n):
            for j in range(1, int(self.indent)):
                tab = tab + " "
        return tab

def usage():
    """ Print the usage """
    print "Usage: xmlcat.py xml_file indentCt(=4) | less"
    print "Use [SPACE] and [CTRL-B] to move forward and backward"
    sys.exit(-1)

def main():
    """ Check the arguments, instantiate the parser and parse """
    narg = len(sys.argv)
    if (narg < 2 or narg > 3):
        usage()
    else:
        file = sys.argv[1]
        indent = 4 if narg == 2 else sys.argv[2]
    parser = make_parser()
    prettyPrintingContentHandler = PrettyPrintingContentHandler(indent)
    parser.setContentHandler(prettyPrintingContentHandler)
    parser.parse(file)

if __name__ == "__main__":
    main()


'''
Given a tree root (or node), displays the tree structure beheath it. 
(showing the number of significant leaves)
'''
# import BeautifulSoup
import bs4
import codecs
# create a default output file. 
# however, Python disallow reassignment of variables inside functions, so we use this hack
outfile = [None]

DEBUG = False

def setOutput(path):
    outfile[0] = codecs.open(path, "w", encoding='utf8')

def myprint(text):
    if not outfile[0]:
        setOutput("treeView.out1")
    outfile[0].write(text + "\n")    
    #print text
    #print "\n"

def displayText(num, id, level):
    ''' Display a Text leafnode:
        Inputs:
        num: number of char in the text
        id: the node id (sibling number)
        level: the depth in the tree
        Output:
        [nodeID] size
    '''
    if (num > 100 or DEBUG):
        outstring = ("  " * (level-1) + "[{}] ({})").format(id, num)
        myprint(outstring)

def display(node, id, level):
    ''' Display a parent node:
        Inputs:
        node:the node object
        id: the nodeID (rank among its sibling)
        level: depth of tree
        Output:
        [nodeID] TAG (numChild) textSize
    '''
    try:
        name = node.name
        lentext = len(node.text)
    except:
        name = type(node)
        lentext = "NA"
    outstring = ("  " * (level-1) + "[{}] {} ({}) {}->").format(id, name, len(node), lentext)  
    if (len(node) > 0 or DEBUG):
        myprint(outstring)

TEXTNODE_TYPES = [bs4.NavigableString, bs4.Comment]
TAG_TYPES = [ bs4.element.Tag]

def traverse(node, id, level=1):
    ''' Procedure to traverses a tree and prints output (default to ./treeView.out1)
        Inputs:
        node: the starting node
        id: the nodeID (rank among starting node's siblings) for record purpose
        level: tree depth, for tab spacing
        Outputs:
        default to file ./tmp1. Can be set via setOutput(path)
    '''
    if ( type(node) not in TAG_TYPES):
        nodelen = len(node)
        displayText(nodelen, id, level)
    elif (len(node) == 1 and type(node.contents[0]) not in TAG_TYPES):
        # minimize printing for singlet parents
        nodelen = len(node.text)
        displayText(nodelen, id, level)
    else:
        display(node, id, level)
        clevel = level+1

        childrenCt = len(node)
        for cid in range(childrenCt):
            child = node.contents[cid]
            traverse(child, cid, clevel)

def getIx(node, coordList, printPath=False):
    '''Get a descendant node from a starting ancestor node and a list of coordinates:
    Inputs:
    node: starting ancestor node
    coordList: e.g. [1,2,3,2,1], the path of child IDs toward the desired descendant
    printPath: print the intermediate node info
    Output:
    descendantNode
    '''
    level=0
    while len(coordList)>0:
        idx=coordList.pop(0)
        if (node and node.contents and len(node.contents)>idx):
            node=node.contents[idx]
            if printPath:
                print ("    "*level+"[{}] name={} {}").format(idx, node.name, node.attrs) 
        else:
            node = None
            break
        level+=1
    return node



def utf8Request(req):
    '''usage: 
	req = requests.get(url) 
	ucode = utf8Request(req)
    '''
    return req.content.decode("utf8")

def cleanTDnet(ucode):
    '''TDnet landing page contains bad html code that messes up BeautifulSoup parser '''
    u2=ucode.replace('<!-I_TMP_TABLE_FORMAT_01.html start->','').replace('<!-I_TMP_TABLE_FORMAT_01.html end->','')
    u2=u2.replace('<!-I_TMP_TABLE_FORMAT_11.html start->','').replace('<!-I_TMP_TABLE_FORMAT_11.html end->','')
    u2=u2.replace('<!-I_TMP_LIST_FRAME.html end->','').replace('<!-I_TMP_LIST_FRAME.html start->','')
    return u2

 
usage='''
cd ~/download_daily/www.aastocks.com/20150120.0/en%LTP/
import pickle
t1=pickle.load(open("RTAI.aspx?type=1", 'r'))
import bs4 
cd ~/tasks/201501.sites/
import treeViewer
treeViewer.traverse(t1.contents[3],3,1)
treeViewer.outfile.close()


t11=getIx(t1,[3,3,3,3,1,19,3,1,3,1,1,1,7,1,1,1,1,0,2])
''' 

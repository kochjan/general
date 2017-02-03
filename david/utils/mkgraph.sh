#!/bin/bash

/opt/python2.7/bin/python make_graph.py > tmp.dot
dot -Tpng tmp.dot -o ~/shared/daily_proc.png

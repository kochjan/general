#!/opt/python2.7/bin/python2.7
"""
backup 3 task tables every hour
"""
import nipun.dbc as dbc

dbo = dbc.db(connect='qai')

t = ['task_definition', 'task_item', 'task_dependency']

sql_drop = """drop table dwang..%(table)s"""

sql = """select * into dwang..%(table)s from production_task..%(table)s"""


for i in t:
    try:
        s = sql_drop%{'table':i}
        print s
        dbo.cursor.execute(s)
        dbo.commit()
    except Exception,e:
        print e
    try:
        s = sql%{'table':i}
        print s
        dbo.cursor.execute(s)
        dbo.commit()
    except Exception,e:
        print e

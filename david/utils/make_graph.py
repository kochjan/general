import pandas
import nipun.dbc as dbc
dbo = dbc.db(connect='gce-data')

restrict='npxchnpak'
     
sql = '''select distinct ti.task_detail, task_class
        , task_dependency
        , frequency, friday
        from production_task.task_definition td join 
        production_task.task_item ti on td.task_name=ti.task_name
        where ti.task_detail!='weekly_itg_download'
        and (task_detail like '%{x}%'
        or task_dependency like '%{x}%')
        order by task_class, friday
        '''.format(x=restrict)


data = dbo.query(sql, df=True)

ix = data['frequency'].isnull()

monthly = data[-ix]
daily = data[ix]

tree_string = ''
tree_tail = ''


tree_head = ''' digraph G {
    ratio="auto"
'''

def clean_tree(x,y):
    global tree_string
    global tree_tail 
    if '%s -> %s' % (x,y) in tree_string:
        tree_string = tree_string.replace('%s -> %s' % (x,y), ' %s [color=black]; %s -> %s [color=red]; %s' % (x, x,y , y))
    else:
        tree_tail += '%s -> %s [color=red];\n' % (x,y)

for row in daily[daily['task_dependency'].notnull()][['task_detail', 'task_dependency']].values:
    detail = row[0]
    depends = map(lambda x: x.strip(), row[1].split(','))
    dep = lambda x: clean_tree(x, detail)
    map(dep, depends)

tree_tail += ''' } '''
print tree_head + tree_string + tree_tail


#############################################################################
'''
#数据源
dataSource = {
    '14': {'host': '172.18.2.14', 'userName': 'omniv5', 'password': 'ABCabc123', 'dataBase': 'omniv5_wms'},
    '15': {'host': '172.18.2.15', 'userName': 'omniv5', 'password': 'ABCabc123', 'dataBase': 'omniv5_wms'},
    '21': {'host': '172.18.2.21', 'userName': 'omniv5', 'password': 'ABCabc123', 'dataBase': 'omniv5_wms'},
}

#Key
server = '21'

host = dataSource[server]['host']
userName = dataSource[server]['userName']
passWord = dataSource[server]['password']
dataBase = dataSource[server]['dataBase']


#tables = ['outboundorder', 'osoorder','offshelforder','rmaorder','revorder']
tables = ['revorder']

print("current database info ==>host:%s,userName:%s,password:%s,dataBase:%s" %
      (host, userName, passWord, dataBase))
'''
#############################################################################


import yaml

import logging



#读取文件
f = open('config.yaml')

#导入
x = yaml.load(f)


host = x['hostName']
userName = x['userName']
passWord = x['password']
dataBase = x['dataBase']




#tables = ['outboundorder', 'osoorder','offshelforder','rmaorder','revorder']
#tables = ['revorder']

tables = x['tables']
print(tables)

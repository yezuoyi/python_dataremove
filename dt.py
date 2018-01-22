#!/usr/bin/python3
#coding:utf-8

import pymysql

import config

import logging

import datetime


createdDate = input("输入日期：")



logging.basicConfig(filename=str(createdDate)+'.log',
                    format='%(asctime)s -%(name)s-%(levelname)s-%(module)s:%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S %p',
                    level=logging.DEBUG)




# 打开数据库连接
db = pymysql.connect(config.host, config.userName,
                     config.passWord, config.dataBase, charset='utf8')

# 使用cursor()方法获取操作游标
cursor = db.cursor()

#查出tableName的依懒关系
def getDendencyInfo(tableName):
    sql = "select \
    TABLE_NAME,COLUMN_NAME,CONSTRAINT_NAME, REFERENCED_TABLE_NAME,REFERENCED_COLUMN_NAME \
    from INFORMATION_SCHEMA.KEY_COLUMN_USAGE \
    where \
    CONSTRAINT_SCHEMA = '%s' and REFERENCED_TABLE_NAME = '%s'" % (config.dataBase, tableName)

    print(sql)
    logging.info(sql)

    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        results = cursor.fetchall()
        resultList = [];
        for row in results:
            record = list(row);
            resultList.append(record)
        return resultList
    except:
        print("Error: unable to fetch data")
        logging.info("Error: unable to fetch data")

#删除指定记录
def deleteRecords(tableName,column,value):
    sql = "delete from %s where %s = %d" % (tableName,column,value)
    try:
        # 执行SQL语句
        print(sql)
        logging.info(sql)
        cursor.execute(sql)
        # 提交修改
        db.commit()
    except:
        # 发生错误时回滚
        db.rollback()


#一次查出tableName的表
def queryAllDependency(tableName,totalRecords):
    tmpRecords = getDendencyInfo(tableName)
    if tmpRecords:
        totalRecords.extend(tmpRecords);
       # print("totalRecords =",totalRecords)
        for recorde in tmpRecords:
            queryAllDependency(str(recorde[0]), totalRecords)
    else:
        return;

#删除当前记录
def deleteCurrentRecord(tableName, primaryKeyId):
    getSqlFromQuery(tableName,primaryKeyId)
    sql = "delete from %s where id= %Ld" % (tableName, primaryKeyId)
    try:
        # 执行SQL语句
        print(sql)
        logging.info(sql)
        cursor.execute(sql)
        # 提交修改
        db.commit()
    except:
        # 发生错误时回滚
        db.rollback()
        print("some error .")

#查出主键和外键的依懒关系
def queryPrimaryKey(record, id):
    sql = "select id from %s where %s = %Ld" % (record[0], record[1], id)
    try:
        print(sql)
        logging.info(sql)
        cursor.execute(sql)
        results = []
        result = cursor.fetchall()
        for row in result:
            results.append(row[0])
        return results
    except:
        print("no data")
        logging.info("no data")

#将依懒关系保存到本地内存，不用每删除一条记录要与数据库交互一次
def getDendencyInfoLocalList(tableName, list):
    results = []
    for record in list:
        if record[3] == str(tableName):
            results.append(record)
    return results

#递归处理每一天记录，直到找没有外键依懒的进行删除


def processRecord(tableName, primaryKeyId):
    result = getDendencyInfoLocalList(tableName, totalRecords)
    #print(result)
    if result:
        for record in result:
            recordIds = queryPrimaryKey(record, primaryKeyId)
            print("recordIds==", recordIds)
            logging.info("recordIds==%s", str(recordIds))
            try:
                for recordId in recordIds:
                    processRecord(record[0], recordId)
            except:
                print("no record will be delete")
                logging.info("no record will be delete..")

        deleteCurrentRecord(tableName, primaryKeyId)

    else:
            # 为空时，删除这些表
        deleteCurrentRecord(tableName, primaryKeyId)




def getIds(tableName,createdDate):
    #sql = 'select * from outboundorder where createdDate < '2017 - 06 - 16''
    sql = "select id from %s where createdDate < '%s'"%(tableName,createdDate)
    try:
        print(sql)
        logging.info(sql);
        cursor.execute(sql)
        results = []
        result = cursor.fetchall()
        for row in result:
            results.append(row[0])
        return results
    except:
        print("no data")
        logging.info("no data")


def saveSqlToFile(sqlSaveToFile):
    sqlSaveToFile = [str(x) + "\n" for x in sqlSaveToFile]
    fo.writelines(sqlSaveToFile)
    

def getSqlFromQuery(tableName, primaryKeyId):
    sql = "select * from %s where id = %Ld" %(tableName,primaryKeyId)
    try:
         # 执行SQL语句
        cursor.execute(sql)
         # 获取所有记录列表
        results = cursor.fetchall()
        resultList = list(results[0])
        dateList = [x.strftime('%Y-%m-%d %H:%M:%S') if isinstance(x, datetime.datetime) else x for x in resultList]
        sql1 = str(dateList).replace("None", "NULL").replace("[", "(").replace("]", ");")
        field_names = [i[0] for i in cursor.description]
        sql2 = "insert into %s (%s) VALUES " % ( tableName,",".join(field_names))
        sql3 = sql2 + sql1
        sqlSaveToFile.append(sql3)
    except:
        print("no sql need to be saved")
        logging.info("no sql need to be saved")
        

    


totalRecords = []

sqlSaveToFile = []



fo = open(str(createdDate)+".sql", "w")

for table in config.tables:
    print("current table %s is processing...",table)
    logging.info("current table %s is process...",table)
    totalRecords.clear()
    #从数据库里查出tableName表，所有的依懒关系，保存到totalRecords
    ids = getIds(table,createdDate)
    if ids:
        queryAllDependency(table, totalRecords)
        print(totalRecords)
        logging.info(totalRecords)
        print("-----------------")
        logging.info("--------------")
        for id in ids:
            sqlSaveToFile.clear()
            processRecord(table, id)
            sqlSaveToFile.reverse()
            saveSqlToFile(sqlSaveToFile)
    else:
        print("table no data will be process==>",table)
        logging.info("table no data will be process==>%s",str(table))

            



#从数据库里查出tableName表，所有的依懒关系，保存到totalRecords
#queryAllDependency(tableName, totalRecords)

#print(totalRecords)

#print("-----------------")

fo.close()
# 关闭数据库连接
db.close()

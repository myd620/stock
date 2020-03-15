from time import *
import pandas as pd
import tushare as ts
from datetime import date
import datetime
import pymysql

mysql_conn = pymysql.connect('localhost','root','','stockDataBase')
cursor = mysql_conn.cursor()


#-------------------------------定义函数：计算当天往前推Deltadays个交易日所对应的日期--------------------------
def com_StartDate(Deltadays):
    i=0
    da=date.today()
    while i!=Deltadays:
        da=da-datetime.timedelta(days=1)
        if da.isoweekday()==6 or da.isoweekday()==7:   #判断当前日期是否为周末（此处未考虑法定节假日，后续改进）
            pass
        else:
            i+=1
    return da.strftime("%Y-%m-%d")   #返回值数据类型为string

#--------------定义函数：判断某支股票处于什么状态，并返回股票代码、名字和股价（对于创新高的股票）等信息--------
def stock_info(stockID,startdate):
    today=date.today()
    df=ts.get_hist_data(stockID,start=startdate,end=today.strftime("%Y-%m-%d")) #获取股票信息
    if type(df)!=type(None) and len(df.index)>1:                       #抓取时间周期内，有数据天数低于2天的，算作新股
        period_close = df['close'].max()
        close = df['close'][0]
        return  stockID,today.strftime("%Y-%m-%d"), close, period_close

def update_stock_to_mysql(stockID,name):
    today=date.today()
    print("proc %s"% stockID)
    sql = "select * from stock where code = %s" % stockID
    cursor.execute(sql)
    results = cursor.fetchall()
    has_record = False
    last_date = ''
    max_price = 0
    max_price_date = ''
    startDate = ""
    if len(results) != 0:
        last_date = results[0][2]
        max_price = results[0][3]
        max_price_date = results[0][4]
        has_record = True

    if has_record:
        startDate = last_date
    else:
        startDate = com_StartDate(60)

    df=ts.get_hist_data(stockID,start=startdate,end=today.strftime("%Y-%m-%d"))
    if type(df)!=type(None) and len(df.index)>0:
        max_price = df['close'].max()
        max_price_date = df['close'].idxmax()
        close = df['close'][0]
        close_date = df.index[0]
        if has_record:
            sql = "update  stock set closed=%f, close_date='%s', max_price=%f, max_price_date='%s' where code = %s" % (close,close_date,max_price ,max_price_date, stockID)
        else:
            sql = "insert  into stock  values(%s, '%s',%f,'%s',%f,'%s')" % (stockID,name,close,close_date,max_price, max_price_date)
        cursor.execute(sql)
        mysql_conn.commit()




#-----------------------------------------------获取沪深两市所有 上市公司基本信息---------------------------
all_stocks_info=ts.get_stock_basics()
#print(all_stocks_info)
Deltadays=60
startdate=com_StartDate(Deltadays)
today=date.today()

"""
print('---------11111-------')
data = ts.get_hist_data('600000', start='2020-01-05', end='2020-03-15')
print(data)
column_list = []
for row in data:
    column_list.append(row)

print(column_list)
print(data['open'].size)
#print(data['open'][1]) # 第2个数据
"""

index = 0
for i in all_stocks_info.index:
    name = all_stocks_info['name'][index]
    update_stock_to_mysql(str(i),name)
    index = index+1

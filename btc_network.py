import requests
import json
import logging
import pymysql
import os
import yaml
import time
import datetime
from lxml import etree
from apscheduler.schedulers.background import BackgroundScheduler

# 日志配置
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s (filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='btc_network.log',
                    filemode='a')


# 解析yaml
cur_path = os.path.dirname(os.path.realpath(__file__))
x = yaml.load(open('%s/config.yml' % cur_path, encoding='UTF-8'))
# 数据库
host = x['DATADB']['MYSQL']['HOST']
username = x['DATADB']['MYSQL']['UNAME']
pwd = x['DATADB']['MYSQL']['PWD']
database = x['DATADB']['MYSQL']['DNAME']
add_ls = x['BTC_LS']['ADDRESS']


# 数据库连接
def connect_db():
    logging.info('start to connect mysql')
    db = pymysql.connect('{}'.format(host), '{}'.format(username), '{}'.format(pwd), '{}'.format(database))
    logging.info('connect success')
    return db


# 插入数据
def insert_db1(db, address, finalBinance):
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    # SQL 插入语句
    sql = "INSERT INTO btc_address (address,finalBinance) VALUES (%s, %s)"
    par = (address, finalBinance)
    try:
        # 执行sql语句
        cursor.execute(sql, par)
        # 提交到数据库执行
        db.commit()
        logging.info("insert1 success")
    except Exception as e:
        # Rollback in case there is any error
        logging.error(e)
        db.rollback()


# 插入前三条数据
def insert_db2(db, id, block_id, hash, inOrOut, address, amount, webTime):
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    # SQL 插入语句
    sql = "INSERT INTO btc_transact (id,block_id,hash,inOrOut,address,amount,webTime) VALUES (%s, %s, %s, %s, %s, %s, %s) on duplicate key update id = values(id)"
    par = (id, block_id, hash, inOrOut, address, amount, webTime)
    try:
        # 执行sql语句
        cursor.execute(sql, par)
        # 提交到数据库执行
        db.commit()
        logging.info("insert2 success")
    except Exception as e:
        # Rollback in case there is any error
        logging.error(e)
        db.rollback()


# 抓取btc地址页存正式避雷针
def crawl(u):
    url = 'https://www.blockchain.com/btc/address/' + u
    try:
        res = requests.get(url)
    except Exception as e:
        logging.error(e)
        time.sleep(2)
        return None
    html = etree.HTML(res.content)
    address = html.xpath("//tr/td[contains(@class, 'stack-mobile')][2]/a[contains(@class, 'mobile-f12')]")[0].text
    finalBinance = html.xpath("//td[@id='final_balance']/font/span")[0].text
    return address, finalBinance


# 抓取btc地址页第一页
def crawl_ls():
    for u in add_ls:
        url = 'https://www.blockchain.com/btc/address/'+u
        try:
            res = requests.get(url)
        except Exception as e:
            logging.error(e)
            time.sleep(2)
            continue
        html = etree.HTML(res.content)
        db = connect_db()
        for i in range(50):
            hash = html.xpath("//tr[1]/th/a[contains(@class, 'hash-link')]")[i].text
            amount = html.xpath("//div/button/span")[i].text
            webTime = html.xpath("//tr[1]/th/span[contains(@class, 'pull-right')]")[i].text
            address = u
            block_id = ""
            amount = amount.replace("BTC", "").replace(",", "")
            amount = float(amount)
            if amount > 0:
                inOrOut = "0"
            else:
                inOrOut = "1"
            amount = abs(amount)
            if amount < 0.001:
                continue
            else:
                id = address + hash + inOrOut
                insert_db2(db, id, block_id, hash, inOrOut, address, amount, webTime)
        db.close()
        logging.info("table2 close")
        time.sleep(5)


# btc_address启动函数
def run():
    db = connect_db()  # 连接MySQL数据库
    for u in add_ls:
        try:
            address, finalBinance = crawl(u)
            insert_db1(db, address, finalBinance)
        except Exception as e:
            logging.error(e)
            continue
    db.close()
    logging.info("table1 close")

# 非阻塞
SCHEDULER = BackgroundScheduler()
if __name__ == "__main__":
    SCHEDULER.add_job(func=run, trigger='interval', minutes=2)
    SCHEDULER.add_job(func=crawl_ls, trigger='interval', minutes=2)
    SCHEDULER.start()

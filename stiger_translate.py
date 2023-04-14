import translators as ts
import pymysql
import datetime
import os
import logging
import sys

bing_language_dict = {
    'US': 'en',
    'UK': 'en',
    'CA': 'en',
    'DE': 'de',
    'IT': 'it',
    'FR': 'fr',
    'ES': 'es',
    'MX': 'es',
    'JP': 'ja',
    'NL': 'nl',
    'PL': 'pl',
    'SE': 'sw',
    'TR': 'tr'
}

sogou_language_dict = {
    'US': 'en',
    'UK': 'en',
    'CA': 'en',
    'DE': 'de',
    'IT': 'it',
    'FR': 'fr',
    'ES': 'es',
    'MX': 'es',
    'JP': 'ja',
    'NL': 'nl',
    'PL': 'pl',
    'SE': 'sv',
    'TR': 'tr'
}

to_day = datetime.datetime.now()
log_file_path = 'translate_{}_{}_{}.log'.format(to_day.year, to_day.month, to_day.day)

basedir = os.path.abspath("..") #返回脚本所在的绝对路径
log_dir = os.path.join(basedir, '../logs')  # 日志文件所在目录,即‘脚本路径/logs'
if not os.path.isdir(log_dir):
    os.mkdir(log_dir)

logging.basicConfig(level=logging.DEBUG,
                    filename=os.path.join(log_dir, log_file_path),
                    datefmt='%Y/%m/%d %H:%M:%S',
                    format='%(asctime)s | [%(levelname)s] : %(message)s')
                    # format='%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(module)s - %(message)s')
logger = logging.getLogger(__name__)

# StreamHandler
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(level=logging.DEBUG)
formatter = logging.Formatter('%(asctime)s | [%(levelname)s] : %(message)s')
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


# 生产环境
# MYSQL_HOST = ''
# MYSQL_DBNAME = ''
# MYSQL_USER = ''
# MYSQL_PASSWORD = ''
# MYSQL_PORT =

# 测试环境
MYSQL_HOST = 'localhost'
MYSQL_DBNAME = 'amazon_db'
MYSQL_USER = 'root'
MYSQL_PASSWORD = '12345678'
MYSQL_PORT = 3306

# 打开数据库连接
conn = pymysql.connect(host=MYSQL_HOST,
                       port=MYSQL_PORT,
                       user=MYSQL_USER,
                       passwd=MYSQL_PASSWORD,
                       db=MYSQL_DBNAME,
                       charset='utf8'
                       )

# 使用 cursor() 方法创建一个游标对象 cursor

cursor = conn.cursor()
logger.info('连接生产库成功！')
# 使用 execute()  方法执行 SQL 查询
# cursor.execute("show databases;")
# cursor.execute("use database_name;")
# cursor.execute("show tables;")
cursor.execute("""
                select 
                a.id,
                a.site_name_en,
                a.customer_comments
                from amazon_customer_returns_cn a
                left join (
                select id 
                from amazon_customer_returns_cn
                where 1=1
                and char_length(customer_comments_cn)!=length(customer_comments_cn)
                ) b on a.id = b.id
                where 
                b.id is null or a.id is null
               """
               )


data = cursor.fetchall()
logger.info('获取需翻译数据行数：' + str(len(data)))
logger.info('开始翻译处理')
success_cnt = 0
for item in data:
    # 暂时做2个翻译api，应该够用，不够时候再加 by Patrick
    targe_string = item[2].replace('&#39;', '\'').replace('�', '\'')
    try:
        comment_cn = ts.sogou(targe_string, sogou_language_dict[item[1]], 'zh-CN')
        logger.info('处理评论id:' + str(item[0]) + ',使用搜狗API')
    except Exception as e:
        comment_cn = ts.bing(targe_string, bing_language_dict[item[1]], 'zh-CN')
        logger.info('处理评论id:' + str(item[0]) + ',使用BingAPI')
    cursor.execute("update amazon_customer_returns_cn set customer_comments_cn = '"
                   + comment_cn
                   + "' where id = '"
                   + item[0] + "'"
                   )
    conn.commit()
    success_cnt = success_cnt + 1
#
# 关闭数据库连接
logger.info('关闭数据库链接')
logger.info('本次一共处理成功行数：' + str(success_cnt))
cursor.close()
conn.close()

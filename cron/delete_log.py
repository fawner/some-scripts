# -*- coding:utf-8 -*-
# 定时任务
# 删除指定数据库中一个月前的日志
# 需要自定义配置
import datetime
import pymysql
import contextlib

# 空白变量，用于默认变量
_no_value = object()


def get_some_days_ago_time(days):
    """
    获取当前时间days天前的日期
    :return: str '2010-10-10 00:00:00'
    """
    return ' '.join([str(datetime.datetime.now().date() - datetime.timedelta(days=days)), '00:00:00'])


# 定义上下文管理器，连接后自动关闭连接
@contextlib.contextmanager
def get_mysql(*value, host=_no_value, port=_no_value, user=_no_value, passwd=_no_value,
              db=_no_value, charset=_no_value):
    """
    获取数据库连接
    :param value:
    :param host: 数据库ip
    :param port: 数据库使用端口
    :param user: 连接数据库使用的账户名称
    :param passwd: 账户密码
    :param db: 必要参数，要连接的数据库
    :param charset:
    :return:
    """
    if db is _no_value:
        assert False
    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        passwd=passwd,
        db=db,
        charset=charset
    )
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
    try:
        yield cursor
    finally:
        conn.commit()
        cursor.close()
        conn.close()


def write_log(cursor):
    """
    自定义查询，记录日志
    :param cursor: 游标
    :return: function
    """

    def execute(sql):
        start = datetime.datetime.now()
        effect_row = cursor.execute(sql)
        end = datetime.datetime.now()
        print(Config.log_temp.format(time=str(start), sql=sql, time_consuming=end - start, effect_row=effect_row))

    return execute


class Config(object):
    """配置类"""
    # 删除语句
    sql_delete = 'delete from {table} where id <= {max_id} order by id limit {limit_number}'
    # 查找需要删除的数据中最大的id
    sql_find_max_id = 'select max(id) from {table} where {condition}'
    # 符合条件的数据的数量
    sql_select_len = 'select count(id) from {table} where {condition}'
    # 日志模板
    log_temp = """
    ### time: {time}
    sql: {sql}
    time consuming: {time_consuming}
    effect_row: {effect_row}
    """
    will_delete_day = get_some_days_ago_time(30)
    limit_number = 10000
    # 每次最大删除数量
    delete_max_number = 500000
    # table_config = { key: value } key is table_name, value like { 'time': condition field }
    # like table_config = {'table_name': {'time': 'in_time' }, ... }
    table_config = {}
    # mysql配置项
    # 适用于单服务器。
    mysql_host = '127.0.0.1'
    mysql_port = 3306
    mysql_user = 'root'
    mysql_passwd = 'root'
    # 数据库名称必须填写
    mysql_db = _no_value
    mysql_charset = 'utf8'


def main():
    with get_mysql(host=Config.mysql_host, port=Config.mysql_port, user=Config.mysql_user, passwd=Config.mysql_passwd,
                   db=Config.mysql_db, charset=Config.mysql_charset) as cursor:
        implment = write_log(cursor)
        config = Config()
        # 清理数据
        for table_name in config.table_config:
            conf = config.table_config[table_name]

            sql = config.sql_find_max_id.format(table=table_name,
                                                condition='{} < "{}"'.format(conf['time'], config.will_delete_day))
            implment(sql)
            max_id = cursor.fetchone()['max(id)']

            sql = config.sql_select_len.format(table=table_name, condition='id<={}'.format(str(max_id)))
            implment(sql)
            remaining_amount = cursor.fetchone()['count(id)']

            sql = config.sql_delete.format(table=table_name, max_id=max_id, limit_number=config.limit_number)
            delete_max_number = config.delete_max_number
            while remaining_amount > 0 and delete_max_number > 0:
                implment(sql)
                remaining_amount -= config.limit_number
                delete_max_number -= config.limit_number


if __name__ == '__main__':
    main()

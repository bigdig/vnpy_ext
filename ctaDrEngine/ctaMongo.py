# encoding: UTF-8

import multiprocessing
import traceback

import pymongo

from ctaAlgo.ctaBase import CtaBarData
from dataRecorder.drBase import DrTickData

# 数据库写入进程
_db_write_proc = None

# 数据库写入进程的任务队列
_db_write_task_queue = None

# 数据库写入进程停止符
STOP_CTAMONGO_QUEUE = ('STOP_CTAMONGO_QUEUE', None)


def init_db_write_process():
    """初始化数据库写入进程

    :return:
    """
    global _db_write_proc, _db_write_task_queue
    if not _db_write_proc:
        _db_write_task_queue = multiprocessing.Queue()
        _db_write_proc = multiprocessing.Process(target=_do_db_write_task, args=(_db_write_task_queue,))
        _db_write_proc.daemon = True
        _db_write_proc.start()


def _post(*task):
    """数据库写入任务推送

    :param task:
    :return:
    """
    try:
        _db_write_task_queue.put_nowait(task)
    except:
        traceback.print_exc()


def _make_db_conn():
    """生成新的数据库连接"""
    return pymongo.MongoClient()


def _do_db_write_task(queue):
    """数据库写入任务执行引擎

    :param queue: 数据库写入任务队列
    :return:
    """
    conn = _make_db_conn()
    while True:
        try:
            func, args = queue.get()
            if func == STOP_CTAMONGO_QUEUE[0]:
                break
            globals()[func](conn, *args)
        except:
            traceback.print_exc()


def upsert_tick(dbname, colname, tick):
    """更新tick数据库
    任务将被推送至数据库写入进程异步执行。

    :param dbname: 数据库名
    :param colname: 集合名
    :param tick: tick数据
    :return:
    """
    _post(_upsert_tick_task.__name__, (dbname, colname, tick))


def upsert_kline(dbname, colname, kline):
    """更新K线数据库
    任务将被推送至数据库写入进程异步执行。

    :param dbname: 数据库名
    :param colname: 集合名
    :param kline: K线数据
    :return:
    """
    _post(_upsert_klines_task.__name__, (dbname, colname, kline))


def _upsert_tick_task(conn, dbname, colname, tick):
    """tick数据库更新任务
    该任务在数据库写入进程中同步执行。

    :param conn: 数据库连接
    :param dbname: 数据库名
    :param colname: 集合名
    :param tick: tick数据
    :return:
    """
    try:
        dr_tick = DrTickData()
        dr_tick.__dict__.update(tick.__dict__)
        flt = dict(datetime=dr_tick.datetime)
        col = conn[dbname][colname]
        col.replace_one(flt, dr_tick.__dict__, upsert=True)
    except:
        traceback.print_exc()


def _upsert_klines_task(conn, dbname, colname, kline):
    """K线数据库更新任务
    该任务在数据库写入进程中同步执行。

    :param conn: 数据库连接
    :param dbname: 数据库名
    :param colname: 集合名
    :param kline: K线数据
    :return:
    """
    try:
        bar = CtaBarData()
        bar.vtSymbol = kline.symbol
        bar.symbol = kline.symbol
        bar.open = float(kline.open)
        bar.high = float(kline.high)
        bar.low = float(kline.low)
        bar.close = float(kline.close)
        bar.date = kline.datetime.date().strftime('%Y%m%d')
        bar.time = kline.datetime.time().isoformat()
        bar.datetime = kline.datetime
        bar.volume = kline.volume

        # 在线生成的K线额外记录open和close的时间，用于重启程序后能够继续更新K线。
        # 主要针对跨交易时间段的周期。
        bar.open_datetime = kline.open_datetime
        bar.close_datetime = kline.close_datetime

        flt = dict(datetime=bar.datetime)
        col = conn[dbname][colname]
        col.replace_one(flt, bar.__dict__, upsert=True)
    except:
        traceback.print_exc()


def find_last_klines(dbname, colname, count, from_datetime):
    """检索某一时间点之前的最新历史K线

    :param dbname: 数据库名
    :param colname: 集合名
    :param count: 获取K线的数目
    :param from_datetime: 条件时间点，检索结果不包含以该时间结束的K线
    :return: 结果按时间逆序排列
    """
    if 'conn' not in find_last_klines.__dict__:
        find_last_klines.__dict__['conn'] = _make_db_conn()
    conn = find_last_klines.__dict__['conn']

    col = conn[dbname][colname]

    return list(col.find(filter={'datetime': {'$lt': from_datetime}},
                         projection={'_id': False},
                         limit=count,
                         sort=(('date', pymongo.DESCENDING),
                               ('time', pymongo.DESCENDING))))

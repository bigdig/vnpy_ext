# encoding: UTF-8

import copy
import datetime as dt
import json
import os
from collections import defaultdict

from dataRecorder import drEngine
from . import ctaKLine, ctaMongo

# 默认采集周期，仅在无法读取配置文件时有效
DEFAULT_PERIODS = (ctaKLine.PERIOD_1MIN,
                   ctaKLine.PERIOD_15MIN,
                   ctaKLine.PERIOD_30MIN,
                   ctaKLine.PERIOD_60MIN)

# 数据采集配置文件
CONFIG_FILE = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'CTADR_setting.json')


class CtaDrEngine(drEngine.DrEngine):
    """数据采集引擎
    继承自vnpy提供的drEngine.DrEngine，加入或更改了以下行为：
    1. 生成多周期K线，通过注册回调方式提供K线完成通知。
    2. 实时历史K线获取功能，可获取指定数目的最新历史K线。
    3. 覆盖原生DrEngine的insertData方法，数据库写入由子类负责实现多进程异步。
    """

    def __init__(self, mainEngine, eventEngine):
        """初始化

        :param mainEngine: 参照父类
        :param eventEngine: 参照父类
        """
        super(CtaDrEngine, self).__init__(mainEngine, eventEngine)

        # 启动数据库异步写入进程
        ctaMongo.init_db_write_process()

        try:
            # 从配置文件中获取需要的采集周期
            with open(CONFIG_FILE) as fp:
                settings = json.load(fp)
                self.kline_periods = settings['recording_kline_periods']
                self.recording_tick = settings['recording_tick']
        except:
            self.kline_periods = DEFAULT_PERIODS
            self.recording_tick = False

        # K线生成器
        self.kline_gen = ctaKLine.KLineGenerator(periods=self.kline_periods,
                                                 recording_tick=self.recording_tick)

        # K线完成事件回调集合，合约代码 => 采集周期 => 回调列表
        self.kline_completed_listeners = defaultdict(lambda: defaultdict(list))

    def insertData(self, dbName, collectionName, data):
        """屏蔽父类的数据库写入行为

        :param dbName: 参照父类
        :param collectionName: 参照父类
        :param data: 参照父类
        :return:
        """
        pass

    def procecssTickEvent(self, event):
        """处理tick数据
        使用tick更新K线，在K线完成时执行回调。
        暂不屏蔽父类向界面输出数据的行为。

        :param event: 参照父类
        :return:
        """
        super(CtaDrEngine, self).procecssTickEvent(event)

        # 获取tick的拷贝，对其进行修改
        tick = copy.deepcopy(event.dict_['data'])

        # 将tick中的字母信息统一修改为大写
        tick.symbol = tick.symbol.upper()
        tick.exchange = tick.exchange.upper()
        tick.vtSymbol = tick.vtSymbol.upper()

        # 提前计算tick时间
        tick.datetime = dt.datetime.strptime(' '.join([tick.date, tick.time]), '%Y%m%d %H:%M:%S.%f')

        # 更新K线
        updated_klines = self.kline_gen.update(tick, self.activeSymbolDict)
        if not updated_klines:
            return

        # K线完成时执行回调
        for p in self.kline_periods:
            if updated_klines[p].is_completed:
                map(lambda callback: callback(updated_klines[p].updated_kline),
                    self.kline_completed_listeners[updated_klines[p].updated_kline.symbol][p])

    def registerKlineCompletedEvent(self, symbol, period_callback_dict):
        """注册K线完成事件回调

        :param symbol: 交易的合约代码
        :param period_callback_dict: 采集周期对应回调的字典
        :return:
        """
        for k, v in period_callback_dict.items():
            self.kline_completed_listeners[symbol.upper()][k].append(v)

    def removeKlineCompletedEvent(self, symbol, period_callback_dict):
        """注销K线完成事件回调

        :param symbol: 交易的合约代码
        :param period_callback_dict: 采集周期对应回调的字典
        :return:
        """
        for k, v in period_callback_dict.items():
            self.kline_completed_listeners[symbol.upper()][k].remove(v)

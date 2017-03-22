# encoding: UTF-8

import bisect
import datetime as dt
from collections import namedtuple

from .ctaConstant import *

"""
【交易时间线】
交易时间线用于描述期货品种在一天时间内的交易时间段， 程序内部表示为由数个时间点组成的列表。
每个时间点包含时间本身以及开始/停止标识符。

例如：
    [(21:00, 开始), (23:00, 停止),
     ( 9:00, 开始), (10:15, 停止),
     (10:30, 开始), (11:30, 停止),
     ...]

【tick过滤】
tick过滤使用期货品种对应的交易时间线，从中检索tick时间之前最接近（可相等）的时间点。
如时间点被标识为开始，则tick在有效交易时间内，反之无效。
"""

# 交易开始/停止标识符，用于识别时间点的含义
OPEN, CLOSE = True, False

# 交易时间点
Tradetime = namedtuple('Tradetime', 'time oc')

# 小时偏移量
# 由于夜盘如果跨越12点（例：21:00 - 2:30），会导致时间点逆序，
# 因此在计算时，时间线和实时tick数据统一加上小时偏移量，使时间严格升序。
HOUR_BIAS = 6


def hour_bias_helper(t, b=HOUR_BIAS):
    """小时偏移辅助函数
    本函数使用replace对datetime.time进行处理，
    datetime.datetime应直接使用datetime.timedelta处理。

    :param t: 偏移对象，datetime.time
    :param b: 小时偏移量
    :return: 偏移HOUR_BIAS后的同类型对象
    """
    return t.replace(hour=(t.hour + b) % 24)


# 默认日盘时间
DAYTIME_DEFAULT = map(lambda t: Tradetime(hour_bias_helper(t.time), t.oc), (
    Tradetime(dt.time(hour=9, minute=0), OPEN),
    Tradetime(dt.time(hour=10, minute=15), CLOSE),
    Tradetime(dt.time(hour=10, minute=30), OPEN),
    Tradetime(dt.time(hour=11, minute=30), CLOSE),
    Tradetime(dt.time(hour=13, minute=30), OPEN),
    Tradetime(dt.time(hour=15, minute=0), CLOSE),
))

# 日盘时间，CFFEX股指（Index Future）
DAYTIME_CFFEX_IF = map(lambda t: Tradetime(hour_bias_helper(t.time), t.oc), (
    Tradetime(dt.time(hour=9, minute=30), OPEN),
    Tradetime(dt.time(hour=11, minute=30), CLOSE),
    Tradetime(dt.time(hour=13, minute=0), OPEN),
    Tradetime(dt.time(hour=15, minute=0), CLOSE),
))

# 日盘时间，CFFEX国债（Treasury Bond）
DAYTIME_CFFEX_TB = map(lambda t: Tradetime(hour_bias_helper(t.time), t.oc), (
    Tradetime(dt.time(hour=9, minute=15), OPEN),
    Tradetime(dt.time(hour=11, minute=30), CLOSE),
    Tradetime(dt.time(hour=13, minute=0), OPEN),
    Tradetime(dt.time(hour=15, minute=15), CLOSE),
))

# 夜盘时间，SHFE，AU、AG
NIGHTTIME_SHFE_1 = map(lambda t: Tradetime(hour_bias_helper(t.time), t.oc), (
    Tradetime(dt.time(hour=21, minute=0), OPEN),
    Tradetime(dt.time(hour=2, minute=30), CLOSE),
))

# 夜盘时间，SHFE，CU、AL、ZN、PB、SN、NI
NIGHTTIME_SHFE_2 = map(lambda t: Tradetime(hour_bias_helper(t.time), t.oc), (
    Tradetime(dt.time(hour=21, minute=0), OPEN),
    Tradetime(dt.time(hour=1, minute=0), CLOSE),
))

# 夜盘时间，SHFE，RU、RB、HC、BU
NIGHTTIME_SHFE_3 = map(lambda t: Tradetime(hour_bias_helper(t.time), t.oc), (
    Tradetime(dt.time(hour=21, minute=0), OPEN),
    Tradetime(dt.time(hour=23, minute=0), CLOSE),
))

# 夜盘时间，DCE
NIGHTTIME_DCE = map(lambda t: Tradetime(hour_bias_helper(t.time), t.oc), (
    Tradetime(dt.time(hour=21, minute=0), OPEN),
    Tradetime(dt.time(hour=23, minute=30), CLOSE),
))

# 夜盘时间，CZCE
NIGHTTIME_CZCE = map(lambda t: Tradetime(hour_bias_helper(t.time), t.oc), (
    Tradetime(dt.time(hour=21, minute=0), OPEN),
    Tradetime(dt.time(hour=23, minute=30), CLOSE),
))

# 各交易所日盘商品交易时间线，含小时偏移量
TRADING_TIMELINES = {
    EXCHANGE_UNKNOWN: DAYTIME_DEFAULT,
    EXCHANGE_SHFE: DAYTIME_DEFAULT,  # 上期所
    EXCHANGE_DCE: DAYTIME_DEFAULT,  # 大商所
    EXCHANGE_CZCE: DAYTIME_DEFAULT,  # 郑商所
    EXCHANGE_CFFEX: dict(  # 中金所
            IF=DAYTIME_CFFEX_IF,  # 股指
            TB=DAYTIME_CFFEX_TB,  # 国债
    ),
}

# 有夜盘品种的交易时间线，由夜盘+日盘组合而成，含小时偏移量
TRADING_TIMELINES_WITH_NIGHTTIME = [
    NIGHTTIME_SHFE_1 + DAYTIME_DEFAULT,
    NIGHTTIME_SHFE_2 + DAYTIME_DEFAULT,
    NIGHTTIME_SHFE_3 + DAYTIME_DEFAULT,
    NIGHTTIME_DCE + DAYTIME_DEFAULT,
    NIGHTTIME_CZCE + DAYTIME_DEFAULT,
]

# 有夜盘的品种代码到时间线的映射
NIGHTTIME_CODE_MAPPING = {
    # 上期所
    CODE_AU: 0, CODE_AG: 0,
    CODE_CU: 1, CODE_AL: 1, CODE_ZN: 1, CODE_PB: 1, CODE_SN: 1, CODE_NI: 1,
    CODE_RU: 2, CODE_RB: 2, CODE_HC: 2, CODE_BU: 2,
    # 大商所
    CODE_P: 3, CODE_J: 3, CODE_M: 3, CODE_Y: 3, CODE_A: 3, CODE_B: 3, CODE_JM: 3, CODE_I: 3,
    # 郑商所
    CODE_SR: 4, CODE_CF: 4, CODE_RM: 4, CODE_MAPTA: 4, CODE_ZC: 4, CODE_FG: 4, CODE_OI: 4,
}


def timeline_for_tick(tick):
    """从TRADE_TIMES中获取该tick所属的时间线
    判定仅使用tick中的symbol和exchange属性，其余属性均不使用。

    :param tick: VtTickData
    :return: 时间线列表（[TradeTime, ...]）
    """
    # 从合约代码中提取品种
    code = tick.symbol.strip().rstrip('0123456789').upper()

    # 有夜盘的品种
    if code in NIGHTTIME_CODE_MAPPING:
        return TRADING_TIMELINES_WITH_NIGHTTIME[NIGHTTIME_CODE_MAPPING[code]]

    # 日盘品种
    if tick.exchange in TRADING_TIMELINES:
        if tick.exchange == EXCHANGE_CFFEX:  # TODO 暂未实现识别方法
            raise NotImplementedError('中金所股指、国债暂不支持。')
        return TRADING_TIMELINES[tick.exchange]

    # TODO 该判断方法属于权宜之计，今后有时间一定要将所有品种逐一映射到时间线

    raise LookupError('找不到Tick数据对应的交易时间线。')


def is_valid_tick(tick):
    """验证Tick是否在有效交易时间段

    :param tick: VtTickData
    :return: True/False
    """
    timeline = timeline_for_tick(tick)
    # print(tick.vtSymbol, timeline)

    # 取时间部分，加上时间偏移量
    tick_time = Tradetime(hour_bias_helper(tick.datetime.time()), OPEN)

    # 在时间线中用二分法检索tick所在时间范围的起始
    # 如tick时间在一天的交易时间之前则idx为-1，由于Python索引的特性，
    # idx自动指向时间线末尾的收盘时间点，因此不存在问题。
    idx = bisect.bisect_right(timeline, tick_time) - 1
    # print(timeline[idx])

    return timeline[idx].oc

# encoding: UTF-8

import bisect
import datetime as dt
import itertools
from collections import (
    OrderedDict,
    defaultdict,
    namedtuple
)

from . import ctaMongo
from . import ctaTimeline

# K线周期常量
(PERIOD_1MIN,
 PERIOD_2MIN,
 PERIOD_3MIN,
 PERIOD_5MIN,
 PERIOD_15MIN,
 PERIOD_30MIN,
 PERIOD_60MIN,
 PERIOD_120MIN,
 PERIOD_240MIN,
 PERIOD_1DAY) = range(10)

# K线周期对应的分钟数常量
MINUTES_OF_PERIOD = (
    1,  # PERIOD_1MIN
    2,  # PERIOD_2MIN
    3,  # PERIOD_3MIN
    5,  # PERIOD_5MIN
    15,  # PERIOD_15MIN
    30,  # PERIOD_30MIN
    60,  # PERIOD_60MIN
    120,  # PERIOD_120MIN
    240,  # PERIOD_240MIN
    1440,  # PERIOD_1DAY
)

# 数据库名
TICK_DB_NAME = 'VnTrader_Tick_Db'
KLINE_DB_NAMES = {
    PERIOD_1MIN: 'VnTrader_1Min_Db',
    PERIOD_2MIN: 'VnTrader_2Min_Db',
    PERIOD_3MIN: 'VnTrader_3Min_Db',
    PERIOD_5MIN: 'VnTrader_5Min_Db',
    PERIOD_15MIN: 'VnTrader_15Min_Db',
    PERIOD_30MIN: 'VnTrader_30Min_Db',
    PERIOD_60MIN: 'VnTrader_60Min_Db',
    PERIOD_120MIN: 'VnTrader_120Min_Db',
    PERIOD_240MIN: 'VnTrader_240Min_Db',
    PERIOD_1DAY: 'VnTrader_Daily_Db',
}

# 初始化时预读K线的数目
INIT_KLINE_COUNT = 10

# 单个K线生成器的K线最大缓存数目
MAX_KLINE_COUNT = 100000

KLineTuple = namedtuple('KLineTuple', 'updated_kline is_completed')


class KLineGenerator(object):
    """K线生成器类"""

    def __init__(self, periods=(PERIOD_1MIN,), recording_tick=False, ignore_past=True):
        """初始化

        :param periods: 使用K线周期常量指定需要生成的特定周期K线，默认只生成1分钟K线
        :param ignore_past: 如果为True，则该生成器将记忆实例化时间，并过滤该时间之前的tick
        """
        # 存放特定周期K线生成器的字典容器
        self.kline_gens = {prd: KLineGenImpl(prd) for prd in periods}

        # 是否将tick记录到数据库
        self.recording_tick = recording_tick

        # 依据ignore_past设置时间卫兵，卫兵之前的tick视作无效tick
        self.datetime_guard = dt.datetime.now() if ignore_past else dt.datetime.min

        # 存放各合约最后一个tick中的当日总成交量信息，用于计算差值得出每个tick所包含的成交量
        self.last_daily_volumes = {}

    def update(self, tick, active_dict):
        """实时更新K线值

        :param tick: VtTickData，合约代码、交易所等含字母的信息必须为大写
        :param active_dict: 主力合约对应表
        :return: 如果tick为有效数据，返回特定周期K线的字典，{PERIOD: KLineTuple(KLINE, STATUS), ...}，其中：
                 - PERIOD K线周期常量
                 - KLINE  KLine类实例
                 - STATUS True/False -> 完整/更新中
                 如果tick为非交易时间段的无效数据，返回None
        """
        # 检验tick是否为有效数据
        if tick.datetime >= self.datetime_guard and ctaTimeline.is_valid_tick(tick):
            # 计算tick的交易量
            # TODO 该算法会导致程序启动后第一个tick、交易日第一个tick的交易量被忽略
            last_volume = self.last_daily_volumes.get(tick.symbol, tick.volume)
            tick.lastVolume = max(tick.volume - last_volume, 0)  # 跨交易日的时候成交量大小会反转，导致出现负值
            self.last_daily_volumes[tick.symbol] = tick.volume

            # 将tick记录到数据库中
            if self.recording_tick:
                ctaMongo.upsert_tick(TICK_DB_NAME, tick.symbol, tick)
                if tick.symbol in active_dict:
                    ctaMongo.upsert_tick(TICK_DB_NAME, active_dict[tick.symbol], tick)

            # 更新容器中所有K线生成器，并返回所有得到的K线
            updated_klines = {prd: gen.update(tick) for prd, gen in self.kline_gens.items()}

            # 将K线记录到数据库
            for prd, kline in updated_klines.items():
                ctaMongo.upsert_kline(KLINE_DB_NAMES[prd], tick.symbol, kline.updated_kline)
                if tick.symbol in active_dict:
                    ctaMongo.upsert_kline(KLINE_DB_NAMES[prd], active_dict[tick.symbol], kline.updated_kline)

            return updated_klines
        else:
            return None

    def get_last_klines(self, symbol, count, period=PERIOD_1MIN, only_completed=True, newest_tick_datetime=None):
        """获取一定数量的过去K线

        :param symbol: 合约代码
        :param count: K线数目
        :param period: K线周期常量，默认为1分钟
        :param only_completed: 是否跳过更新中的K线只获取已完成的K线，默认为跳过
        :param newest_tick_datetime: 调用此函数时最新tick的时间，结束时间小于该时间的K线将被判定为已完成。
                                     默认使用当前本地时间，由于tick会延时到达，可能会造成将尚未更新完的K线返回，
                                     但在网络条件较好的情况下，影响很小，可以忽略。
                                     可以通过手动传递最新到达的tick时间来彻底防止这个问题。
        :return:
        """
        return self.kline_gens[period].get_last_klines(
                symbol, count, only_completed,
                newest_tick_datetime=newest_tick_datetime if newest_tick_datetime else dt.datetime.now())


class KLine(object):
    """K线类"""

    def __repr__(self):
        return '[datetime={}, VtSymbol={}, Symbol={},' \
               ' Open={} <{}>, High=<{}>, Low=<{}>, Close={} <{}>, Volume=<{}>]'.format(
                self.datetime,
                self.vtSymbol, self.symbol,
                self.open_datetime, self.open,
                self.high, self.low,
                self.close_datetime, self.close,
                self.volume)

    def __init__(self, datetime):
        """初始化

        :param datetime: K线时间。
                         日线以下为K线的结束时间；
                         日线为K线交易日的零时。
        """
        self.datetime = datetime

        self.vtSymbol = ''  # vt系统代码
        self.symbol = ''  # 代码

        # OHLC
        self.open = 0
        self.high = 0
        self.low = 0x7FFFFFFFF
        self.close = 0

        # 存储OC的时间戳，用于接收tick数据时更新OC值
        self.open_datetime = dt.datetime.max
        self.close_datetime = dt.datetime.min

        self.volume = 0  # 成交量
        # self.openInterest = 0  # 持仓量

    def update(self, tick):
        """更新K线值

        :param tick: VtTickData
        :return:
        """
        # 更新OHLC
        if tick.datetime < self.open_datetime:
            self.open = tick.lastPrice
            self.open_datetime = tick.datetime
        if tick.datetime > self.close_datetime:
            self.close = tick.lastPrice
            self.close_datetime = tick.datetime
        self.high = max(self.high, tick.lastPrice)
        self.low = min(self.low, tick.lastPrice)

        # 更新成交量
        self.volume += tick.lastVolume


class KLineGenImpl(object):
    """K线生成器具体实现"""

    kline_pattern_1 = {PERIOD_1MIN, PERIOD_3MIN, PERIOD_5MIN, PERIOD_15MIN}
    kline_pattern_2 = {PERIOD_2MIN, PERIOD_30MIN, PERIOD_60MIN, PERIOD_120MIN, PERIOD_240MIN}
    kline_pattern_3 = PERIOD_1DAY

    def __init__(self, period):
        """初始化

        :param period: K线周期常量
        """
        assert PERIOD_1MIN <= period <= PERIOD_1DAY
        self.klines = defaultdict(OrderedDict)  # 各品种短周期K线容器，以symbol为键，每条线按时间顺序存放
        self.period = period

    def update(self, tick):
        """实时更新K线值

        :param tick: VtTickData，合约代码、交易所等含字母的信息必须为大写
        :return: 如创建新K线，且上一根K线更新完毕，则返回 -> KLineTuple(完成后的K线, True)
                 上述情况以外，均返回 -> KLineTuple(更新中的K线, False)
        """
        # 方法返回值
        # updated_kline, is_completed = None, False

        if not tick.datetime:
            tick.datetime = dt.datetime.strptime(' '.join([tick.date, tick.time]), '%Y%m%d %H:%M:%S.%f')

        # 如缓存中不存在K线，则尝试从数据库中获取一部分K线
        if not self.klines[tick.symbol]:
            self.get_last_klines(tick.symbol, INIT_KLINE_COUNT)

        kline_datetime = self._calc_kline_datetime(tick)

        if kline_datetime not in self.klines[tick.symbol]:  # 需要创建新的K线
            # 创建新的K线并用tick更新
            new_kline = KLine(kline_datetime)
            new_kline.symbol = tick.symbol
            new_kline.vtSymbol = tick.vtSymbol
            new_kline.update(tick)

            if len(self.klines[tick.symbol]) == 0:  # 若无历史K线
                updated_kline, is_completed = new_kline, False
            else:  # 将队列中最后一根K线作为已完成K线返回
                updated_kline, is_completed = self.klines[tick.symbol].values()[-1], True

            # 将K线放入容器，并确保新创建的K线不会破坏容器内的顺序
            self.klines[tick.symbol][kline_datetime] = new_kline
            if len(self.klines[tick.symbol]) > 1:
                last_but_one, last_one = self.klines[tick.symbol].keys()[-2:]
                if last_but_one > last_one:  # 违反时间顺序
                    self.klines[tick.symbol] = OrderedDict(sorted(self.klines[tick.symbol].items()))

            # 如果K线数量大于缓存最大容量，从最老的记录开始删除
            while len(self.klines[tick.symbol]) > MAX_KLINE_COUNT:
                self.klines[tick.symbol].popitem()
        else:  # 更新既有K线
            self.klines[tick.symbol][kline_datetime].update(tick)
            updated_kline, is_completed = self.klines[tick.symbol][kline_datetime], False

        return KLineTuple(updated_kline, is_completed)

    def get_last_klines(self, symbol, count, only_completed=True, newest_tick_datetime=None):
        """获取一定数量的过去K线

        :param symbol: 合约代码
        :param count: K线数目
        :param only_completed: 是否跳过更新中的K线只获取已完成的K线，默认为跳过
        :param newest_tick_datetime: 调用此函数时最新tick的时间，结束时间小于该时间的K线将被判定为已完成。
                                     默认使用当前本地时间，由于tick会延时到达，可能会造成将尚未更新完的K线返回，
                                     但在网络条件较好的情况下，影响很小，可以忽略。
                                     可以通过手动传递最新到达的tick时间来彻底防止这个问题。
        :return:
        """
        symbol = symbol.upper()

        # 如所需K线不足，从数据库中读取
        if len(self.klines) <= count:
            from_datetime = (self.klines[symbol].values()[0].datetime
                             if self.klines[symbol] else
                             # 考虑跨周末的K线（例如用周五夜盘的tick更新下周一的日线），
                             # 用三天后的时间作为过滤条件
                             dt.datetime.now() + dt.timedelta(days=3))
            for doc in ctaMongo.find_last_klines(
                    KLINE_DB_NAMES[self.period], symbol, count - len(self.klines) + 1, from_datetime):
                # 根据数据库查询结果生成历史K线
                kline = KLine(doc['datetime'])
                kline.vtSymbol = doc['vtSymbol']
                kline.symbol = doc['symbol']
                kline.open = doc['open']
                kline.high = doc['high']
                kline.low = doc['low']
                kline.close = doc['close']
                kline.volume = doc['volume']

                # 如果有open和close的时间，则该记录是在线生成的K线，并且有需要继续更新的可能性。
                kline.open_datetime = doc.get('open_datetime', dt.datetime.min)
                kline.close_datetime = doc.get('close_datetime', dt.datetime.max)

                # 将K线放入容器中
                self.klines[symbol][kline.datetime] = kline

            # 保证容器顺序
            self.klines[symbol] = OrderedDict(sorted(self.klines[symbol].items()))

        if not newest_tick_datetime:
            newest_tick_datetime = dt.datetime.now()

        if only_completed:
            from_idx = len(self.klines[symbol])
            for kline in self.klines[symbol].values()[::-1]:
                if self.period < PERIOD_1DAY:  # 日线以下用K线的结束时间比较
                    if kline.datetime < newest_tick_datetime:
                        break
                elif self.period == PERIOD_1DAY:  # 日线用日期比较
                    # 将tick时间加上偏移量计算出所属日期，考虑跨非工作日的情况
                    tick_date = adjust_to_next_working_day(
                            newest_tick_datetime + dt.timedelta(hours=ctaTimeline.HOUR_BIAS)).date()
                    if kline.datetime.date() < tick_date:
                        break
                else:
                    raise AssertionError('K线周期不存在。')
                from_idx -= 1
            return self.klines[symbol].values()[max(from_idx - count, 0):from_idx]
        else:
            return self.klines[symbol].values()[-count:]

    def _calc_kline_datetime(self, tick):
        """计算K线时间

        :param tick: VtTickData
        :return:
        """
        # 1、3、5、15分钟K线，不会跨交易时间段
        if self.period in self.kline_pattern_1:
            # 以分钟精度计算tick时间
            tick_dt_minute = tick.datetime.replace(second=0, microsecond=0)

            # 计算整周期中的分钟余数
            minute_remainder = (int((tick_dt_minute - dt.datetime.min).total_seconds() // 60) %
                                MINUTES_OF_PERIOD[self.period])

            # 将tick_dt_minute修正到整周期位置
            tick_dt_minute -= dt.timedelta(minutes=minute_remainder)

            # 返回K线的结束时间
            return tick_dt_minute + dt.timedelta(minutes=MINUTES_OF_PERIOD[self.period])

        # 2、30、60、120、240分钟K线，会跨交易时间段和周末非交易日
        if self.period in self.kline_pattern_2:
            # 获取tick品种对应的K线时间线
            timeline = get_kline_timeline(self.period, tick)

            # 以分钟精度计算tick时间
            tick_dt_minute = tick.datetime.replace(second=0, microsecond=0)
            tick_time = ctaTimeline.Tradetime(ctaTimeline.hour_bias_helper(tick_dt_minute.time()), ctaTimeline.OPEN)

            # 使用二分法确定tick所在K线的位置
            start_idx = bisect.bisect_right(timeline, tick_time) - 1
            assert timeline[start_idx].oc

            # 获取K线的结束时间
            timedelta_tick2close = (dt.datetime.combine(dt.datetime.min, timeline[start_idx + 1].time) -
                                    dt.datetime.combine(dt.datetime.min, tick_time.time))
            end_datetime = tick_dt_minute + timedelta_tick2close

            # 对于有夜盘的品种，当最后一根K线跨了交易时间段，则它的结束时间的日期应当为下一个工作日，
            # 否则当下一个工作日的tick到来时无法继续更新最后一根K线。
            # 周一到周四的夜盘，由于结束时间会自动计算到正常工作日，不会出问题。
            # 而对于周五的夜盘，由于跨了周末非交易日，最后一根K线的结束时间需要做特殊处理，否则会落到周六。

            # 首先判断tick为周五夜盘
            if (tick.datetime + dt.timedelta(hours=ctaTimeline.HOUR_BIAS)).weekday() == 5:
                # 获取夜盘结束时间
                nighttime_end = next(itertools.dropwhile(lambda t: t.oc == ctaTimeline.OPEN,
                                                         ctaTimeline.timeline_for_tick(tick)))
                # 若K线开始时间、结束时间跨越了夜盘结束时间点，将K线结束时间调整到工作日
                if timeline[start_idx].time < nighttime_end.time < timeline[start_idx + 1].time:
                    end_datetime = adjust_to_next_working_day(end_datetime)

            # 返回K线的结束时间
            return end_datetime

        # 日线，会跨周末非交易日
        if self.period == self.kline_pattern_3:
            # 将tick时间加上偏移量计算出所属日期，考虑跨非工作日的情况
            tick_dt_date = adjust_to_next_working_day(
                    tick.datetime + dt.timedelta(hours=ctaTimeline.HOUR_BIAS)).replace(
                    hour=0, minute=0, second=0, microsecond=0)

            # 返回K线所在交易日零时
            return tick_dt_date

        raise LookupError('找不到Tick数据对应的K线时间。')


def get_kline_timeline(period, tick):
    """获取中周期（30分钟以上非日线）K线的时间线
    时间线是由Tradetime组成的列表，用于定位tick所属的K线。
    例如（30分钟线）：
        [( 9:00, OPEN),
         ( 9:30, OPEN),
         (10:00, OPEN),
         (10:45, OPEN), // 中间包含15分钟非交易时间
         ...
         (15:00, CLOSE)] // 每个交易时间段结束时间如果同时是K线的结束时间，则安插一个CLOSE时间点作为卫兵使用
    ※注意实际获取的时间线包含大小为ctaTimeline.HOUR_BIAS的小时偏移量。

    :param period: K线周期常量
    :param tick: VtTickData
    :return:
    """
    # 尝试从缓存中获取已计算的结果
    memorize_key = (tick.symbol, period)
    if memorize_key in get_kline_timeline.__dict__:
        return get_kline_timeline.__dict__[memorize_key]

    # 方法返回值
    timeline = []
    # 周期分钟数
    period_minutes = MINUTES_OF_PERIOD[period]

    # 获取该品种交易时间线
    trade_timeline = ctaTimeline.timeline_for_tick(tick)

    # 交易时间线上每个时间段的开始部分，需要补充到上一个时间段最后一根K线的长度
    delta_add_to_last_kline = dt.timedelta()

    # 根据交易时间线上各时间段计算K线时间点
    for idx in range(0, len(trade_timeline), 2):
        start_datetime = dt.datetime.combine(dt.date.min, trade_timeline[idx].time)
        end_datetime = dt.datetime.combine(dt.date.min, trade_timeline[idx + 1].time)
        time_delta = end_datetime - start_datetime

        # 考虑delta_add_to_last_kline大于交易时间段长度的情况
        if delta_add_to_last_kline > time_delta:
            # 更新delta_add_to_last_kline并直接跳入下一个时间段
            delta_add_to_last_kline -= time_delta
            continue

        # 将时间段用周期长度分割
        quot, rem = divmod((time_delta - delta_add_to_last_kline).seconds / 60, period_minutes)

        # 将K线起始时间点加入时间线
        timeline.extend(ctaTimeline.Tradetime(
                (start_datetime + delta_add_to_last_kline + dt.timedelta(minutes=q * period_minutes)).time(),
                ctaTimeline.OPEN) for q in range(quot))

        # 有余数的情况
        if rem > 0:
            # 加入余量部分对应的K线起始时间
            timeline.append(ctaTimeline.Tradetime(
                    (start_datetime + delta_add_to_last_kline + dt.timedelta(minutes=quot * period_minutes)).time(),
                    ctaTimeline.OPEN))
            # 记住下一个时间段中需要补充到该K线的长度
            delta_add_to_last_kline = dt.timedelta(minutes=period_minutes - rem)
        else:
            # 没有余数则加入时间段结束点作为K线结束标志
            # （在时间连续的情况下，能够直接使用下一根K线的起始时间作为本K线的结束时间，
            # 但在没有余量时，由于时间段与段之间不连续，则无法使用下一根K线的开始时间。）
            timeline.append(ctaTimeline.Tradetime(end_datetime.time(), ctaTimeline.CLOSE))

    # 无条件加入最终收盘时间点
    if timeline[-1] != trade_timeline[-1]:
        timeline.append(trade_timeline[-1])

    # 将计算结果存入缓存
    get_kline_timeline.__dict__[memorize_key] = timeline

    return timeline


def adjust_to_next_working_day(datetime):
    """将时间点向后调整至工作日

    :param datetime: dt.datetime or dt.date
    :return:
    """
    while datetime.weekday() in (5, 6):
        datetime += dt.timedelta(days=1)
    return datetime

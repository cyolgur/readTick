import pandas as pd
import numpy as np
import datetime
from readConsecutive import read_consecutive_data


STARTTIME1 = datetime.time(21, 0, 0)
ENDTIME1 = datetime.time(2, 30, 0)
STARTTIME2 = datetime.time(9, 0, 0)
ENDTIME2 = datetime.time(11, 30, 0)
STARTTIME3 = datetime.time(13, 30, 0)
ENDTIME3 = datetime.time(15, 0, 0)


# 降频的方式有多种，这里必须预留好给其他降频方式的改动

def part_sign(a_time):
    if (a_time <= ENDTIME1) or (a_time >= STARTTIME1):
        return 1
    elif (a_time >= STARTTIME2) and (a_time <= ENDTIME2):
        return 2
    elif (a_time >= STARTTIME3) and (a_time <= ENDTIME3):
        return 3
    else:
        return 4


# 在groupby的过程中需谨记：有的日子是没有夜盘的，也就是在groupby的过程中会少一栏1
# 不使用第一个价格作为Open, 而是使用第一分钟的收盘价作为Open
def dowm_sample_3part(data):
    data['time_flag'] = data['TradeTime'].apply(part_sign)
    error_mask = (data['time_flag'] == 4)
    data = data.loc[~error_mask, :]
    data_new = data.groupby(data['time_flag']).agg({'Code': 'first',
                                                    'TradeDate': 'first',
                                                    'OpenPrice': 'first',
                                                    'HighPrice': np.nanmax,
                                                    'LowPrice': np.nanmin,
                                                    'ClosePrice': 'last',
                                                    'TradeVolume': np.nansum,
                                                    'OpenInterest': 'last',
                                                    'Turnover': np.nansum})
    data_new['StableOpenPrice'] = data['ClosePrice'].groupby(data['time_flag']).first()
    if len(data_new) == 3:
        pass
    else:
        data_new = data_new.reindex([1, 2, 3])
        data_new[['Code', 'TradeDate']] = data_new[['Code', 'TradeDate']].fillna(method='backfill')
    data_new.set_index(['TradeDate'], append=True, inplace=True)
    data_new = data_new.swaplevel(0, 1)
    return data_new


# 在计算交易日期时需要谨记： 有的日子是没有夜盘的，因此这段时间的切片获得的数据是空的dataframe
def calcTradePrice(data, tradeMinutes):
    trade_startTime = [STARTTIME1, STARTTIME2, STARTTIME3]
    trade_endTime = [datetime.time(i.hour, i.minute + tradeMinutes, 0) for i in trade_startTime]
    mask_night = (data['TradeTime'] >= trade_startTime[0]) & (data['TradeTime'] <= trade_endTime[0])
    mask_am = (data['TradeTime'] >= trade_startTime[1]) & (data['TradeTime'] <= trade_endTime[1])
    mask_pm = (data['TradeTime'] >= trade_startTime[2]) & (data['TradeTime'] <= trade_endTime[2])
    res = []
    for mask in [mask_night, mask_am, mask_pm]:
        tradePrice = data.loc[mask, :]
        if tradePrice.empty:
            res.append(np.nan)
        elif tradePrice['TradeVolume'].sum() == 0:
            res.append(np.nan)
        else:
            res.append(tradePrice['ClosePrice'].mean())
    return res


def read_and_down_sample(inst, date):
    raw_data = read_consecutive_data(inst, date)
    if raw_data.empty:
        return None
    down_sample_data = dowm_sample_3part(raw_data)
    down_sample_data['TradePrice'] = calcTradePrice(raw_data, tradeMinutes=15)
    return down_sample_data


if __name__ == '__main__':
    print(read_and_down_sample('AG', '20210615'))

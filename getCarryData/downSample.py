import datetime
import pandas as pd
import numpy as np

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
def dowm_sample_3part_all_inst(data):
    data['time_flag'] = data['TradeTime'].apply(part_sign)
    error_mask = (data['time_flag'] == 4)
    data = data.loc[~error_mask, :]

    data_new = data.groupby(['Code','time_flag']).agg({
                                                    'TradeDate': 'first',
                                                    'OpenPrice': 'first',
                                                    'HighPrice': np.nanmax,
                                                    'LowPrice': np.nanmin,
                                                    'ClosePrice': 'last',
                                                    'TradeVolume': np.nansum,
                                                    'OpenInterest': 'last',
                                                    'Turnover': np.nansum})

    data_new['StableOpenPrice'] = data.groupby(['Code','time_flag']).agg({'ClosePrice':'first'})

    return data_new


def fill_index(data_new):
    new_index = data_new.index.levels[0]
    new_index = pd.MultiIndex.from_product([new_index, [1, 2, 3]], names=['Code', 'time_flag'])
    data_new = data_new.reindex(new_index)
    data_new['TradeDate'] = data_new['TradeDate'].fillna(method='backfill')
    return data_new
import numpy as np
import pandas as pd
import os
import datetime
from getRawData import get_1mindata_allcontract, handleData, change1minFormat
from downSample import dowm_sample_3part_all_inst, fill_index
from calcCarry import calcCarryTotal
import rqdatac as rq

NEWSYMBOLS = ['AG', 'AL', 'AU', 'BU', 'C', 'CU', 'EB', 'EG', 'FG',
              'HC', 'I', 'J', 'JM', 'L', 'M', 'MA', 'NI', 'OI', 'P', 'PF', 'PP',
              'RB', 'RM', 'RU', 'SA', 'SC', 'SP', 'TA', 'V', 'Y', 'ZC', 'ZN']
ADDSYMBOLS = ['A', 'CF', 'PB', 'SN', 'SR']
DES_DIR = 'D:/data_carry'


def dateRange(start, end):
    start_datetime = datetime.datetime.strptime(start, '%Y%m%d')
    end_datetime = datetime.datetime.strptime(end, '%Y%m%d')
    totalDays = (end_datetime - start_datetime).days
    resRange = [start_datetime + datetime.timedelta(days=i) for i in range(0, totalDays + 1)]
    return resRange


def get_single_carry(inst, date_str):
    raw_data = get_1mindata_allcontract(inst, date_str)
    if raw_data is None:
        return None
    raw_data = change1minFormat(raw_data)
    raw_data = handleData(raw_data, date_str)
    flag_data = fill_index(dowm_sample_3part_all_inst(raw_data))
    res = calcCarryTotal(flag_data)
    return res


# 由于是add模式，因此mode需要调整成'a', 而header需要调整成None
def get_inst_carry(inst, start, end):
    carry_res = []
    dateList = dateRange(start, end)
    for date in dateList:
        tmp_date_str = datetime.datetime.strftime(date, '%Y%m%d')
        carry_res.append(get_single_carry(inst, tmp_date_str))
    pd.concat(carry_res, axis=0).to_csv(os.path.join(DES_DIR, '{}.csv'.format(inst)),
                                        mode='a',
                                        header=False)
    return


if __name__ == '__main__':
    rq.init()
    for symbol in [ 'A','AG', 'AL', 'AU', 'BU', 'C', 'CU', 'EB', 'EG', 'FG',
                   'HC', 'I', 'J', 'JM', 'L', 'M', 'MA', 'NI', 'OI', 'P', 'PF', 'PP',
                   'RB', 'RM', 'RU', 'SA', 'SC', 'SP', 'TA', 'V', 'Y', 'ZC', 'ZN', 'CF', 'PB', 'SN', 'SR']:
        get_inst_carry(symbol, '20221119', '20230105')

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
    print(flag_data)
    res = calcCarryTotal(flag_data)
    return res


def get_inst_carry(inst, start, end):
    carry_res = []
    dateList = dateRange(start, end)
    for date in dateList:
        tmp_date_str = datetime.datetime.strftime(date, '%Y%m%d')
        carry_res.append(get_single_carry(inst, tmp_date_str))
    pd.concat(carry_res, axis=0).to_csv(os.path.join(DES_DIR, '{}.csv'.format(inst)))
    return


def get_inst_carry_test(inst, start, end):
    carry_res = []
    dateList = dateRange(start, end)
    for date in dateList:
        tmp_date_str = datetime.datetime.strftime(date, '%Y%m%d')
        carry_res.append(get_single_carry(inst, tmp_date_str))

    return pd.concat(carry_res, axis=0)


if __name__ == '__main__':
    rq.init()
    '''
    for symbol in NEWSYMBOLS+ADDSYMBOLS:
        get_inst_carry(symbol, '20100101', '20221118')
    '''
    print(get_inst_carry_test('BU', '20230105', '20230105'))

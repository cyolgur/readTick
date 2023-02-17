import pandas as pd
import numpy as np
import datetime
import os
from downSample import read_and_down_sample
from readConsecutive import DATA_SRC

SYMBOLS = ['AG', 'AL', 'AU', 'BU', 'C', 'CU', 'EB', 'EG', 'FG', 'FU',
           'HC', 'I', 'J', 'JM', 'L', 'M', 'MA', 'NI', 'OI', 'P', 'PF', 'PP',
           'RB', 'RM', 'RU', 'SA', 'SC', 'SP', 'TA', 'V', 'Y', 'ZC', 'ZN']
ADDSYMBOLS = ['A', 'CF', 'PB', 'SN', 'SR']

# 这是对复权的数据进行降频的
SRC_PATH = DATA_SRC
DES_PATH = 'D:/data_3part_consecutive'
'''

# 下面两行是对不复权的数据进行降频的
SRC_PATH = DATA_SRC
DES_PATH = 'D:/data_3part_no_fq'
'''


def downsample_concat_1inst(inst, start, end):
    instDir = os.path.join(SRC_PATH, inst)
    start = pd.to_datetime(start)
    end = pd.to_datetime(end)
    downsample_data = []
    while start < end:
        tdate = datetime.datetime.strftime(start, '%Y%m%d')
        filename = '{}.csv'.format(tdate)
        if os.path.exists(os.path.join(instDir, filename)):
            pass
        else:
            start += datetime.timedelta(days=1)
            continue

        tmp_consecutive = read_and_down_sample(inst, tdate)
        if tmp_consecutive is None:
            continue
        downsample_data.append(tmp_consecutive)
        start += datetime.timedelta(days=1)
    res = pd.concat(downsample_data, axis=0)
    res.to_csv(os.path.join(DES_PATH, '{}.csv'.format(inst)),
               mode='a',
               header=False)
    return


if __name__ == '__main__':
    for inst in SYMBOLS + ADDSYMBOLS:
        print(inst)
        downsample_concat_1inst(inst, '20221119', '20230106')

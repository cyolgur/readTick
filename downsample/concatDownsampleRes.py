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

def downsample_concat_1inst(inst):
    instDir = os.path.join(SRC_PATH, inst)
    fileList = os.listdir(instDir)
    tdateList = [fileName[:-4] for fileName in fileList]
    downsample_data = []
    for tdate in tdateList:
        tmp_consecutive = read_and_down_sample(inst, tdate)
        if tmp_consecutive is None:
            continue
        downsample_data.append(tmp_consecutive)
    res = pd.concat(downsample_data, axis=0)
    res.to_csv(os.path.join(DES_PATH, '{}.csv'.format(inst)))
    return


if __name__ == '__main__':
    for inst in SYMBOLS+ADDSYMBOLS:
        print(inst)
        downsample_concat_1inst(inst)

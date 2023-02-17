import numpy as np
import pandas as pd
import os
import datetime
from calc_fqfactor_daily import read1minData

SRC_PATH = 'D:/data_1min_rq_new'
DES_PATH = 'D:/data_1min_consecutive'
FQ_PATH = 'D:/data_fq_factor'
NEWSYMBOLS = ['AG', 'AL', 'AU', 'BU', 'C', 'CU', 'EB', 'EG', 'FG', 'FU',
              'HC', 'I', 'J', 'JM', 'L', 'M', 'MA', 'NI', 'OI', 'P', 'PF', 'PP',
              'RB', 'RM', 'RU', 'SA', 'SC', 'SP', 'TA', 'V', 'Y', 'ZC', 'ZN']
ADDSYMBOLS = ['A', 'CF', 'PB', 'SN', 'SR']

# 对一个品种的所有数据进行赋权操作，并且写入
def changeData(inst):
    inst_dir = os.path.join(SRC_PATH, inst)
    tdatefilelist = os.listdir(inst_dir)
    tdatelist = [i[:-4] for i in tdatefilelist]
    fq_factor = read_fq_factor(inst)
    for filename, tdate in zip(tdatefilelist, tdatelist):
        old_data = read1minData(inst, tdate, old_or_not=False)
        today_fq_factor = get_fq_factor(fq_factor, tdate)
        old_data[['OpenPrice', 'HighPrice', 'LowPrice', 'ClosePrice']] *= today_fq_factor
        write_consecutive_data(inst, filename, old_data)
    return


def write_consecutive_data(inst, filename, data):
    if not os.path.exists(os.path.join(DES_PATH, inst)):
        os.mkdir(os.path.join(DES_PATH, inst))
    else:
        pass
    data.to_csv(os.path.join(DES_PATH, inst, filename))
    return


# 获取后复权系数表
def read_fq_factor(inst):
    fq_factor = pd.read_csv(os.path.join(FQ_PATH, '{}.csv'.format(inst)),
                            index_col=[0, ],
                            parse_dates=True)
    fq_factor.columns = ['factor',]
    return fq_factor


# 找到某天的赋权系数
def get_fq_factor(fq_factor_df, tdate):
    tdate_datetime = datetime.datetime.strptime(tdate, '%Y%m%d')
    return fq_factor_df.at[tdate_datetime, 'factor']


def main():
    for inst in NEWSYMBOLS:
        changeData(inst)
    return


if __name__ == '__main__':
    for inst in ADDSYMBOLS:
        print(inst)
        changeData(inst)

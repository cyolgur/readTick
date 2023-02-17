# 在过去计算carry的时候，计算的index出现下面的错误：
# 将time_flag的1,2，3 变成了0,1,2
# 现在需要修正如下的错误
# 特别的，FU和ZC已经修改了错误，就不需要再修改了

import numpy as np
import pandas as pd
import os

SRC_DIR = 'd:/data_carry'
SYMBOLS = ['A', 'AG', 'AL', 'AU', 'BU', 'C', 'CF', 'CU',
           'EB', 'EG', 'FG', 'HC', 'I', 'J', 'JM', 'L', 'M', 'MA',
           'NI', 'OI', 'P', 'PB', 'PF', 'PP', 'RB', 'RM', 'RU',
           'SA', 'SC', 'SN', 'SP', 'SR', 'TA', 'V', 'Y', 'ZN']


def read_carry(inst):
    path = os.path.join(SRC_DIR, '{}.csv'.format(inst))
    res = pd.read_csv(path,
                      index_col=[0, 1])
    return res


def change_index(a_idx):
    tdate, time_flag = a_idx
    if time_flag == 0:
        return tdate, 1
    elif time_flag == 1:
        return tdate, 2
    elif time_flag == 2:
        return tdate, 3
    else:
        raise ValueError


def change_index_df(df):
    if 0 not in df.index.levels[1]:
        return df
    df.index = pd.MultiIndex.from_tuples(list(map(change_index, df.index)))
    return df


def write_df(inst, df):
    path = os.path.join(SRC_DIR, '{}.csv'.format(inst))
    df.to_csv(path)
    return


if __name__ == '__main__':
    for inst in SYMBOLS:
        df = read_carry(inst)
        write_df(inst, change_index_df(df))

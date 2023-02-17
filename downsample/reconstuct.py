import numpy as np
import pandas as pd
import os

# 以下两行是对复权数据进行的reconstrct
SRC_DIR = 'D:/data_3part_consecutive'
DES_DIR = 'D:/data_3part_consecutive_by_name'


# 以下两行是对不复权数据进行的reconstrct
# SRC_DIR = 'D:/data_3part_no_fq'
# DES_DIR = 'D:/data_3part_no_fq_by_name'

SYMBOLS = ['AG', 'AL', 'AU', 'BU', 'C', 'CU', 'EB', 'EG', 'FG', 'FU',
           'HC', 'I', 'J', 'JM', 'L', 'M', 'MA', 'NI', 'OI', 'P', 'PF', 'PP',
           'RB', 'RM', 'RU', 'SA', 'SC', 'SP', 'TA', 'V', 'Y', 'ZC', 'ZN']
ADDSYMBOLS = ['A', 'CF', 'PB', 'SN', 'SR']
COLNAMES = ['OpenPrice', 'HighPrice', 'LowPrice',
            'ClosePrice', 'TradeVolume', 'OpenInterest',
            'Turnover', 'TradePrice', 'StableOpenPrice']
NEWCOLNAMES = ['index_open', 'index_high', 'index_low',
               'index_close', 'volume', 'openinterest',
               'turnover', 'trade_price', 'stable_open']


def get_file_path(inst):
    return os.path.join(SRC_DIR, '{}.csv'.format(inst))


def read_inst_data(inst):
    file_path = get_file_path(inst)
    return pd.read_csv(file_path,
                       index_col=[0, 1])


def check_duplicated(seq):
    print(seq.index[seq.index.duplicated()])
    return


def reformat():
    data_all_inst = {}
    data_all_col = {}
    ALLSYMBOLS = (SYMBOLS+ADDSYMBOLS)
    for inst in ALLSYMBOLS:
        data_all_inst[inst] = read_inst_data(inst)
        print(inst)
        check_duplicated(data_all_inst[inst])
    for col, newcol in zip(COLNAMES, NEWCOLNAMES):
        data_all_col[col] = pd.concat({inst: data_all_inst[inst][col] for inst in ALLSYMBOLS}, axis=1)
        data_all_col[col].sort_index(inplace=True)
        data_all_col[col].to_csv(os.path.join(DES_DIR, '{}.csv'.format(newcol)))
    return


if __name__ == '__main__':
    reformat()

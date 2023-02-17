import numpy as np
import pandas as pd
import os
import datetime

# 这是对复权数据的读取
DATA_SRC = 'D:/data_1min_consecutive'

# 这是对不复权数据的读取
# DATA_SRC = 'D:/data_1min_rq_new'

SYMBOLS = ['AG', 'AL', 'AU', 'BU', 'C', 'CU', 'EB', 'EG', 'FG', 'FU',
           'HC', 'I', 'J', 'JM', 'L', 'M', 'MA', 'NI', 'OI', 'P', 'PF', 'PP',
           'RB', 'RM', 'RU', 'SA', 'SC', 'SP', 'TA', 'V', 'Y', 'ZC', 'ZN']
ADDSYMBOLS = ['A', 'CF', 'PB', 'SN', 'SR']

def get_file_path(inst, date_str):
    return os.path.join(DATA_SRC, inst, '{}.csv'.format(date_str))


def read_file(file_path):
    data = pd.read_csv(file_path,
                       index_col=[0, ])
    return data


def handle_data(data):
    data['TradeDate'] = data['TradeDate'].apply(lambda a_str: datetime.datetime.strptime(a_str, '%Y-%m-%d').date())
    # 做不复权的数据需要用这个
    #data['TradeTime'] = data['TradeTime'].apply(
        #lambda a_str: datetime.datetime.strptime(a_str, '%Y-%m-%d %H:%M:%S').time())
    #做复权数据需要用这个
    data['TradeTime'] = data['TradeTime'].apply(lambda a_str: datetime.datetime.strptime(a_str, '%H:%M:%S').time())
    # 价格中包含一些0， 应该将这些0赋值为nan
    priceCol = ['OpenPrice', 'HighPrice', 'LowPrice', 'ClosePrice']
    for col in priceCol:
        data[col] = np.where(data[col] == 0, np.nan, data[col])
    return data


def read_consecutive_data(inst, date_str):
    data = read_file(get_file_path(inst, date_str))
    return handle_data(data)


if __name__ == '__main__':
    print(read_consecutive_data('AG', '20210615'))

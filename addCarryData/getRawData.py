import rqdatac as rq
import numpy as np
import pandas as pd
import datetime


# 下面这个函数要获得某一天内，一个品种所有合约的1min数据
def get_1mindata_allcontract(inst, date_str):
    contracts_list = rq.futures.get_contracts(inst, date=date_str)
    if len(contracts_list) == 0:
        return None

    res = rq.get_price(order_book_ids=contracts_list,
                       start_date=date_str,
                       end_date=date_str,
                       fields=['trading_date', 'open', 'high', 'low', 'close',
                               'volume', 'open_interest', 'total_turnover'],
                       frequency='1m')
    return res


def change1minFormat(df):
    df.reset_index(inplace=True)
    df.rename(mapper={'order_book_id': 'Code',
                      'trading_date': 'TradeDate',
                      'datetime': 'TradeTime',
                      'open': 'OpenPrice',
                      'high': 'HighPrice',
                      'low': 'LowPrice',
                      'close': 'ClosePrice',
                      'volume': 'TradeVolume',
                      'open_interest': 'OpenInterest',
                      'total_turnover': 'Turnover'}, axis=1, inplace=True)
    df = df.reindex(columns=['Code', 'TradeDate', 'TradeTime',
                             'OpenPrice', 'HighPrice', 'LowPrice', 'ClosePrice',
                             'TradeVolume', 'OpenInterest', 'Turnover'])
    return df


# 由于部分合约名称的数字会少一位2，因此处理这些合约名称时，需要补上一个2
def getContract(a_str, date_str):
    contract = "".join(filter(str.isdigit, a_str))
    if len(contract) < 4:
        year_month_str = date_str[:6]
        for add_year in range(0, 10):
            tmp_contract = '20{}{}'.format(add_year, contract)
            if tmp_contract > year_month_str:
                contract = '{}{}'.format(add_year, contract)
                break
    inst = "".join(filter(str.isalpha, a_str)).upper()
    return inst + contract


def get_time(a_str):
    return pd.to_datetime(a_str).time()


def handleData(df, date_str):
    df = df.loc[:, ['Code', 'TradeDate', 'TradeTime', 'OpenPrice', 'HighPrice',
                    'LowPrice', 'ClosePrice', 'TradeVolume', 'OpenInterest', 'Turnover']]
    df.sort_index(inplace=True)
    overflow_flag = (np.abs(df['TradeVolume']) > 2e8)
    df.loc[overflow_flag, ['TradeVolume']] = np.nan
    df.loc[overflow_flag, ['Turnover']] = np.nan
    df['Code'] = df['Code'].apply(lambda code: getContract(code, date_str))
    # print(df['TradeTime'])
    df['TradeTime'] = df['TradeTime'].apply(get_time)
    priceCol = ['OpenPrice', 'HighPrice', 'LowPrice', 'ClosePrice']
    for col in priceCol:
        df[col] = np.where(df[col] == 0, np.nan, df[col])
    return df

import numpy as np
import pandas as pd
import datetime
import os
import rqdatac as rq

PATH = "D:/data_1min_rq_new"
PATH_OLD = 'D:/data_1min_rq_old'
PATH_CONTRACT = 'D:/data_1min_contract'

NEWSYMBOLS = ['AG', 'AL', 'AU', 'BU', 'C', 'CU', 'EB', 'EG', 'FG', 'FU',
              'HC', 'I', 'J', 'JM', 'L', 'M', 'MA', 'NI', 'OI', 'P', 'PF', 'PP',
              'RB', 'RM', 'RU', 'SA', 'SC', 'SP', 'TA', 'V', 'Y', 'ZC', 'ZN']
ADDSYMBOLS = ['A', 'CF', 'PB', 'SN', 'SR']


# 在一个包含所有不同到期日的期货合约中，根据tradeVolume的和筛选主力合约
def getMainContract(allContractDF):
    dfByContract = allContractDF.groupby(by=['Code'])['TradeVolume'].sum()
    return pd.to_numeric(dfByContract).idxmax()


def dateRange(start, end):
    start_datetime = datetime.datetime.strptime(start, '%Y%m%d')
    end_datetime = datetime.datetime.strptime(end, '%Y%m%d')
    totalDays = (end_datetime - start_datetime).days
    resRange = [start_datetime + datetime.timedelta(days=i) for i in range(0, totalDays + 1)]
    return resRange


# 当我们拿到主力合约之后，需要对其进行处理
# handle_data包括两个方面：
# 1.处理成交量、成交额的上溢
# 2.规范合约的名字
def handleData(df, date_str):
    df = df.loc[:, ['Code', 'TradeDate', 'TradeTime', 'OpenPrice', 'HighPrice',
                    'LowPrice', 'ClosePrice', 'TradeVolume', 'OpenInterest', 'Turnover']]
    df.sort_index(inplace=True)
    overflow_flag = (np.abs(df['TradeVolume']) > 2e8)
    df.loc[overflow_flag, ['TradeVolume']] = np.nan
    df.loc[overflow_flag, ['Turnover']] = np.nan
    df['Code'] = df['Code'].apply(lambda code: getContract(code, date_str))
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


# 当我们处理完主力合约之后，需要对其进行写入
def writeData(df, inst, date):
    writeDir = os.path.join(PATH, inst)
    if not os.path.exists(writeDir):
        os.mkdir(writeDir)
    else:
        pass
    df.to_csv(os.path.join(writeDir, '{}.csv'.format(date)))
    return


# 在换月时，需要保存旧合约的数据，对其进行写入
def writeOldData(df, inst, date):
    writeDir = os.path.join(PATH_OLD, inst)
    if not os.path.exists(writeDir):
        os.mkdir(writeDir)
    else:
        pass
    df.to_csv(os.path.join(writeDir, '{}.csv'.format(date)))
    return


# 还需要保存每日的主力合约到底是谁！
# 由于是add模式，因此需要将mode调成'a', 将header调成False
def writeContract(contractList, dateList, inst):
    contract_log = pd.Series(contractList, index=dateList)
    tmp_file = os.path.join(PATH_CONTRACT, '{}.csv'.format(inst))
    contract_log.to_csv(tmp_file, mode='a', header=False)
    return


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


# 这个函数只能获得上次最后一个日期的主力合约是什么
# 因此addBarData的起始日只能是上次已获得数据中的后一日
def getLastMainContract(inst):
    contract_list = pd.read_csv(os.path.join(PATH_CONTRACT, '{}.csv'.format(inst)))
    return contract_list.iloc[-1].to_list()[1]


def get1minBarData(start, end, inst):
    # tmp_main_contract中保存第一个上次末尾时刻的主力合约
    tmp_main_contract = [getLastMainContract(inst)]
    dateList = dateRange(start, end)

    realDateList = []
    # 变量over_count和OVER_THRESH 是用于统计换月的变量
    over_count = 0
    OVER_THRESH = 3
    for tmp_datetime in dateList:
        tmp_date_str = datetime.datetime.strftime(tmp_datetime, '%Y%m%d')
        res = get_1mindata_allcontract(inst, tmp_date_str)

        if res is None:
            continue
        # 获得主力合约数据之后，要先进行处理
        res = change1minFormat(res)
        res = handleData(res, tmp_date_str)
        realDateList.append(tmp_date_str)
        tmpMainContract = getMainContract(res)
        # 虽然getMainContract可以筛选主力合约， 但实际上，新合约的成交量要超过旧合约连续三天才会实现真的换月
        # 在连续计数器中，只要有一天未超过，那么计数器就会重新归0
        if not tmp_main_contract:
            resDF = res.loc[res['Code'] == tmpMainContract]
            tmp_main_contract.append(tmpMainContract)

        elif tmpMainContract > tmp_main_contract[-1]:
            # 只有over_count计数器数到OVER_THRESH时， 才会发生换月
            # 换月时，由于需要计算赋权因子，因此需要将旧合约也一并保存下来
            if over_count == OVER_THRESH:
                resDF = res.loc[res['Code'] == tmpMainContract]
                oldDF = res.loc[res['Code'] == (tmp_main_contract[-1])]
                print(tmpMainContract)
                tmp_main_contract.append(tmpMainContract)
                writeOldData(oldDF, inst, tmp_date_str)
                over_count = 0
            else:
                resDF = res.loc[res['Code'] == (tmp_main_contract[-1])]
                tmp_main_contract.append(tmp_main_contract[-1])
                over_count += 1
        elif tmpMainContract <= tmp_main_contract[-1]:
            over_count = 0
            resDF = res.loc[res['Code'] == (tmp_main_contract[-1])]
            tmp_main_contract.append(tmp_main_contract[-1])
        else:
            continue
        writeData(resDF, inst, tmp_date_str)

    # 在add文件中，由于第一个主力合约是我们通过读取文件人为输入的，因此从第二个主力合约写入
    writeContract(tmp_main_contract[1:], realDateList, inst)
    return


if __name__ == '__main__':
    rq.init()

    for symbol in ['AG', 'AL', 'AU', 'BU', 'C', 'CU', 'EB', 'EG', 'FG', 'FU',
                   'HC', 'I', 'J', 'JM', 'L', 'M', 'MA', 'NI', 'OI', 'P', 'PF', 'PP',
                   'RB', 'RM', 'RU', 'SA', 'SC', 'SP', 'TA', 'V', 'Y', 'ZC', 'ZN', 'CF', 'PB', 'SN', 'SR']:
        get1minBarData('20221119', '20230106', symbol)

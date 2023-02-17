import numpy as np
import pandas as pd
import os
import datetime

SRC_PATH = 'D:/data_1min_rq_new'
OLD_PATH = 'D:/data_1min_rq_old'
FQ_PATH = 'D:/data_fq_factor'
CONTRACT_PATH = 'D:/data_1min_contract'

NEWSYMBOLS = ['AG', 'AL', 'AU', 'BU', 'C', 'CU', 'EB', 'EG', 'FG', 'FU',
              'HC', 'I', 'J', 'JM', 'L', 'M', 'MA', 'NI', 'OI', 'P', 'PF', 'PP',
              'RB', 'RM', 'RU', 'SA', 'SC', 'SP', 'TA', 'V', 'Y', 'ZC', 'ZN']
ADDSYMBOLS = ['A', 'CF', 'PB', 'SN', 'SR']

# 首先，需要读取合约的csv文件
def readContract(inst):
    file_path = os.path.join(CONTRACT_PATH, '{}.csv'.format(inst))
    contract_seq = pd.read_csv(file_path,
                               parse_dates=[0, ],
                               index_col=[0, ])
    contract_seq.columns = ['contract']
    return contract_seq


# 需要筛选出那些已经发生了换月的日期
def findRollDate(contract_seq):
    contract_last = contract_seq.shift(1)
    rollSeq = (contract_seq != contract_last)
    rollSeq.iloc[0, 0] = False
    rollSeq = rollSeq.loc[rollSeq['contract']]

    rollDateIdx = rollSeq.index
    return list(rollDateIdx)


# 需要一个读取一个合约某一日数据的函数
def read1minData(inst, date, old_or_not=False):
    if old_or_not:
        file_path = os.path.join(OLD_PATH, inst, '{}.csv'.format(date))
    else:
        file_path = os.path.join(SRC_PATH, inst, '{}.csv'.format(date))
    res = pd.read_csv(file_path, index_col=[0,])
    res['TradeDate'] = res['TradeDate'].apply(lambda a_str: datetime.datetime.strptime(a_str, '%Y-%m-%d').date())
    res['TradeTime'] = res['TradeTime'].apply(lambda a_str: datetime.datetime.strptime(a_str, '%Y-%m-%d %H:%M:%S').time())
    return res


# 需要判断一个时间点是否属于夜盘的时间点
def isNight(a_time):
    if 8 < a_time.hour < 17:
        return 'day'
    else:
        return 'night'


# 需要判断一个合约某一日的数据是否有夜盘
def isNight_day(res):
    res['night_flag'] = list(map(isNight, res.TradeTime))
    # 有两种情况没有夜盘：
    # 1. 夜盘的成交量全部为0，
    # 2. 压根没有夜盘的时间段， 这种情况一般发生在节后的第一天交易日
    volume_count = res['TradeVolume'].groupby(res['night_flag']).sum()
    if 'night' in volume_count.index:
        if volume_count.loc['night'] == 0:
            return False
        else:
            return True
    else:
        return False


# 给定一个表格，和一个是否有夜盘的标志， 返回这一天如果要换月的twap
def calcTwap(df, night_sign):
    if night_sign:
        STARTTIME = datetime.time(21, 0, 0)
        ENDTIME = datetime.time(21, 15, 0)
    else:
        STARTTIME = datetime.time(9, 0, 0)
        ENDTIME = datetime.time(9, 15, 0)

    twap_time = []
    for timeIdx_time in df.TradeTime:
        if (timeIdx_time >= STARTTIME) & (timeIdx_time <= ENDTIME):
            twap_time.append(1)
        else:
            twap_time.append(0)
    df['twap_time'] = twap_time
    return df['ClosePrice'].loc[df['twap_time'] == 1].mean()


# 需要找出那些日期新旧合约对应时间，并计算出twap之比
def calc_fq_1day(inst, date):
    if type(date) == str:
        pass
    else:
        date = datetime.datetime.strftime(date, '%Y%m%d')
    # 读取新旧合约这一天的数据
    old_contract = read1minData(inst, date, old_or_not=True)
    new_contract = read1minData(inst, date, old_or_not=False)
    night_sign = isNight_day(old_contract) & isNight_day(new_contract)
    return calcTwap(old_contract, night_sign) / calcTwap(new_contract, night_sign)


def main():
    for inst in NEWSYMBOLS+ADDSYMBOLS:
        print(inst)
        contract_seq = readContract(inst)
        roll_date = findRollDate(contract_seq)
        inst_fq = []

        for tdate in list(contract_seq.index):
            if tdate in roll_date:
                inst_fq.append(calc_fq_1day(inst, tdate))
            else:
                inst_fq.append(1)
        fq_factor_daily = pd.Series(inst_fq, contract_seq.index)
        fq_factor_total = fq_factor_daily.cumprod()
        fq_factor_total.to_csv(os.path.join(FQ_PATH, '{}.csv'.format(inst)))
    return


if __name__ == '__main__':
    main()

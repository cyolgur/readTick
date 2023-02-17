import numpy as np
import pandas as pd


# 接下来，对上面的数据进行carry的计算
def splitdf(df):
    idx = pd.IndexSlice
    index_level_1 = df.index.levels[1]
    res = {}
    for index_1 in index_level_1:
        res[index_1] = df.loc[idx[:, index_1], :]
    return res


def diff_month(d1, d2):
    d1 = d1[-4:]
    d2 = d2[-4:]
    d1_year, d1_month = int(d1[:2]), int(d1[2:])
    d2_year, d2_month = int(d2[:2]), int(d2[2:])
    return (d1_year - d2_year) * 12 + d1_month - d2_month


def calcCarry(df):
    # 首先找到主力合约
    max_turnover = df['Turnover'].astype(float).max()
    if np.isnan(max_turnover):
        return [np.nan for _ in range(3)], [np.nan for _ in range(3)]
    thresh_turnover = max_turnover / 100.0
    df = df.loc[(df['Turnover'] > thresh_turnover), :].sort_values('Turnover', ascending=False)

    df = df.iloc[:4, :]
    if len(df) <= 1:
        return [np.nan for _ in range(3)], [np.nan for _ in range(3)]
    close_price_arr = df['ClosePrice'].to_numpy().astype(float)
    res_price = list(close_price_arr[1:] / close_price_arr[0] - 1)
    month_arr = df.index.get_level_values(0).to_numpy()
    res_month_diff = [diff_month(month_arr[i], month_arr[0]) for i in range(1, len(month_arr))]
    while len(res_price) < 3:
        res_price.append(np.nan)
        res_month_diff.append(np.nan)
    return res_price, res_month_diff


def calcCarryTotal(df):
    part_df_dict = splitdf(df)
    res = {}
    for idx, part_df in part_df_dict.items():
        ratio, month = calcCarry(part_df)
        res[idx] = ratio + month
    res_df = pd.DataFrame(res).T
    date = df['TradeDate'].iloc[0]
    new_index = pd.MultiIndex.from_product([[date], res_df.index])
    res_df.index = new_index
    res_df.columns = ['ratio_0', 'ratio_1', 'ratio_2', 'month_0', 'month_1', 'month_2']
    return res_df

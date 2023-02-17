# _*_ coding:utf-8
import json
import pymysql
from enum import Enum
import pandas as pd

class SQLType(Enum):
    mysql_db = 0
    hdf5_db = 1
    influx_db = 2


class MinuteType(Enum):
    one_minute = 0
    three_minute = 1
    five_minute = 2
    ten_minute = 3
    fifteen_minute = 4
    thirty_minute = 5
    one_hour = 6


class Exchange(Enum):
    sse = 0
    szse = 1
    bjse = 2
    shfe = 3
    cffex = 4
    dce = 5
    zce = 6


class SecType(Enum):
    a_stock = 0
    b_stock = 1
    preferred_stock = 2
    TB = 3
    corporate_debt = 4
    convertible_bond = 5
    policy_financial_bonds = 6
    securities_corporate_bonds = 7
    abs = 8
    collateralised_repo = 9
    corporate_bonds = 10
    local_government_debt = 11
    other_debt = 12
    LOF = 13
    ETF = 14
    QDII = 15


class ProductType(Enum):
    index = 0
    stock = 1
    futures = 2
    index_option = 3
    stock_option = 4
    futures_option = 5
    ts = 6


class sql_adapter(object):
    def __init__(self, host, user, passwd):
        self.host = host
        self.user = user
        self.passwd = passwd

    def connect(self, db_name, charset='utf8mb4'):
        self.connect_interface = pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.passwd,
            database=db_name,
            port=3306,
            cursorclass=pymysql.cursors.DictCursor
        )
        self.cursor_interface = self.connect_interface.cursor()

    def re_connect(self, db_name, charset='utf8mb4'):
        self.connect_interface.close()
        self.connect(db_name, charset)

    def get_stock_daily_data(self,
                             column_list,
                             code=None,
                             start=None,
                             end=None,
                             table_name='stockdailydata'):
        if len(column_list) == 0:
            columns = '*'
        else:
            columns = ','.join(column_list)  # column_list.join(',')
        if code is None and start is None and end is None:
            sql = "select %s from %s" % (columns, table_name)
            self.cursor_interface.execute(sql)
            data = self.cursor_interface.fetchall()
            return data
        sql = "select %s from %s" % (columns, table_name)
        if code is not None:
            if sql.find('where') == -1:
                sql = "%s where SecuCode=%s" % (sql, code)
            else:
                sql = "%s and SecuCode=%s" % (sql, code)
        if start is not None:
            if sql.find('where') == -1:
                sql = "%s where TradingDay>=%s" % (sql, start)
            else:
                sql = "%s and TradingDay>=%s" % (sql, start)
        if end is not None:
            if sql.find('where') == -1:
                sql = "%s where TradingDay<=%s" % (sql, end)
            else:
                sql = "%s and TradingDay<=%s" % (sql, end)
        self.cursor_interface.execute(sql)
        data = self.cursor_interface.fetchall()
        return data

    def get_stock_daily_history_data(self,
                                     column_list,
                                     code=None,
                                     start=None,
                                     end=None,
                                     table_name='stockdailydatahistory'):
        return self.get_stock_daily_data(column_list, code, start, end, table_name)

    def get_index_component_weight_data(self,
                                        column_list,
                                        table_name='indexcomponentweight',
                                        index_code=None,
                                        secu_code=None,
                                        secu_abbr=None,
                                        trading_day=None,
                                        industry=None):
        if len(column_list) == 0:
            columns = '*'
        else:
            columns = ','.join(column_list)
        if index_code is None and secu_code is None and secu_abbr is None and trading_day is None and industry is None:
            sql = "select %s from %s" % (columns, table_name)
            self.cursor_interface.execute(sql)
            data = self.cursor_interface.fetchall()
            return data
        sql = "select %s from %s" % (columns, table_name)
        if index_code is not None:
            if sql.find('where') == -1:
                sql = "%s where IndexCode=%s" % (sql, index_code)
            else:
                sql = "%s and IndexCode=%s" % (sql, index_code)
        if secu_code is not None:
            if sql.find('where') == -1:
                sql = "%s where SecuCode=\'%s\'" % (sql, secu_code)
            else:
                sql = "%s and SecuCode=\'%s\'" % (sql, secu_code)
        if secu_abbr is not None:
            if sql.find('where') == -1:
                sql = "%s where SecuAbbr=%s" % (sql, secu_abbr)
            else:
                sql = "%s and SecuAbbr=%s" % (sql, secu_abbr)
        if trading_day is not None:
            if sql.find('where') == -1:
                sql = "%s where TradingDay=%s" % (sql, trading_day)
            else:
                sql = "%s and TradingDay=%s" % (sql, trading_day)
        if industry is not None:
            if sql.find('where') == -1:
                sql = "%s where Industry=\'%s\'" % (sql, industry)
            else:
                sql = "%s and Industry=\'%s\'" % (sql, industry)
        self.cursor_interface.execute(sql)
        data = self.cursor_interface.fetchall()
        return data

    def get_stock_data(self,
                       column_list,
                       table_name='allstock',
                       secu_code=None,
                       secu_abbr=None,
                       market=None,
                       string_secu_code=None):
        if len(column_list) == 0:
            columns = '*'
        else:
            columns = ','.join(column_list)

        if secu_code is None and secu_abbr is None and market is None and string_secu_code is None:
            sql = "select %s from %s" % (columns, table_name)
            self.cursor_interface.execute(sql)
            data = self.cursor_interface.fetchall()
            return data
        sql = "select %s from %s" % (columns, table_name)
        if secu_code is not None:
            if sql.find('where') == -1:
                sql = "%s where SecuCode=\'%s\'" % (sql, secu_code)
            else:
                sql = "%s and SecuCode=\'%s\'" % (sql, secu_code)
        if secu_abbr is not None:
            if sql.find('where') == -1:
                sql = "%s where SecuAbbr=%s" % (sql, secu_abbr)
            else:
                sql = "%s and SecuAbbr=%s" % (sql, secu_abbr)
        if string_secu_code is not None:
            if sql.find('where') == -1:
                sql = "%s where StringSecuCode=\'%s\'" % (sql, string_secu_code)
            else:
                sql = "%s and StringSecuCode=\'%s\'" % (sql, string_secu_code)
        if market is not None:
            if sql.find('where') == -1:
                sql = "%s where Market=%s" % (sql, market)
            else:
                sql = "%s and Market=%s" % (sql, market)
        self.cursor_interface.execute(sql)
        data = self.cursor_interface.fetchall()
        return data

    def get_industry_classify_data(self,
                                   column_list,
                                   table_name='industryclassify',
                                   secu_code=None,
                                   trading_day=None,
                                   zx_first_industry=None,
                                   start=None,
                                   end=None):
        if len(column_list) == 0:
            columns = '*'
        else:
            columns = ','.join(column_list)
        if secu_code is None and trading_day is None and zx_first_industry is None and start is None and end is None:
            sql = "select %s from %s" % (columns, table_name)
            self.cursor_interface.execute(sql)
            data = self.cursor_interface.fetchall()
            return data
        sql = "select %s from %s" % (columns, table_name)
        if secu_code is not None:
            if sql.find('where') == -1:
                sql = "%s where SecuCode=\'%s\'" % (sql, secu_code)
            else:
                sql = "%s and SecuCode=\'%s\'" % (sql, secu_code)
        if zx_first_industry is not None:
            if sql.find('where') == -1:
                sql = "%s where ZxFirstIndustry=\'%s\'" % (sql, zx_first_industry)
            else:
                sql = "%s and ZxFirstIndustry=\'%s\'" % (sql, zx_first_industry)
        if trading_day is not None:
            if sql.find('where') == -1:
                sql = "%s where TradingDay=\'%s\'" % (sql, trading_day)
            else:
                sql = "%s and TradingDay=\'%s\'" % (sql, trading_day)
            self.cursor_interface.execute(sql)
            data = self.cursor_interface.fetchall()
            return data
        if start is not None:
            if sql.find('where') == -1:
                sql = "%s where TradingDay>=\'%s\'" % (sql, start)
            else:
                sql = "%s and TradingDay>=\'%s\'" % (sql, start)
        if end is not None:
            if sql.find('where') == -1:
                sql = "%s where TradingDay<=\'%s\'" % (sql, end)
            else:
                sql = "%s and TradingDay<=\'%s\'" % (sql, end)
        self.cursor_interface.execute(sql)
        data = self.cursor_interface.fetchall()
        return data

    def get_stock_share_data(self,
                             column_list,
                             secu_code=None,
                             trading_day=None,
                             start=None,
                             end=None,
                             table_name='stockshare'):
        if len(column_list) == 0:
            columns = '*'
        else:
            columns = ','.join(column_list)
        if secu_code is None and trading_day is None and start is None and end is None:
            sql = "select %s from %s" % (columns, table_name)
            self.cursor_interface.execute(sql)
            data = self.cursor_interface.fetchall()
            return data
        sql = "select %s from %s" % (columns, table_name)
        if secu_code is not None:
            if sql.find('where') == -1:
                sql = "%s where SecuCode=\'%s\'" % (sql, secu_code)
            else:
                sql = "%s and SecuCode=\'%s\'" % (sql, secu_code)
        if trading_day is not None:
            if sql.find('where') == -1:
                sql = "%s where TradingDay=\'%s\'" % (sql, trading_day)
            else:
                sql = "%s and TradingDay=\'%s\'" % (sql, trading_day)
            self.cursor_interface.execute(sql)
            data = self.cursor_interface.fetchall()
            return data
        if start is not None:
            if sql.find('where') == -1:
                sql = "%s where TradingDay>=\'%s\'" % (sql, start)
            else:
                sql = "%s and TradingDay>=\'%s\'" % (sql, start)
        if end is not None:
            if sql.find('where') == -1:
                sql = "%s where TradingDay<=\'%s\'" % (sql, end)
            else:
                sql = "%s and TradingDay<=\'%s\'" % (sql, end)
        self.cursor_interface.execute(sql)
        data = self.cursor_interface.fetchall()
        return data

    def close(self):
        self.cursor_interface.close()
        self.connect_interface.close()

    def get_minute_data(self,
                        table_name,
                        column_list=[],
                        code=None,
                        start=None,
                        end=None,
                        other=None):
        if len(column_list) == 0:
            columns = '*'
        else:
            columns = ','.join(column_list)
        if code is None and start is None and end is None:
            sql = "select %s from %s" % (columns, table_name)
            result = self.cursor_interface.execute(sql)
            data = self.cursor_interface.fetchall()
            return data
        sql = "select %s from %s" % (columns, table_name)
        if code is not None:
            if sql.find('where') == -1:
                sql = "%s where thscode=\'%s\'" % (sql, code)
            else:
                sql = "%s and thscode=\'%s\'" % (sql, code)
        if start is not None:
            start_year = start[0:4]
            start_mon = start[4:6]
            start_day = start[6:]
            if sql.find('where') == -1:
                sql = "%s where date_trade>=\'%s-%s-%s 00:00:00\'" % (sql, start_year, start_mon, start_day)
            else:
                sql = "%s and date_trade>=\'%s-%s-%s 00:00:00\'" % (sql, start_year, start_mon, start_day)
        if end is not None:
            end_year = end[0:4]
            end_mon = end[4:6]
            end_day = end[6:]
            if sql.find('where') == -1:
                sql = "%s where date_trade<=\'%s-%s-%s 00:00:00\'" % (sql, end_year, end_mon, end_day)
            else:
                sql = "%s and date_trade<=\'%s-%s-%s 00:00:00\'" % (sql, end_year, end_mon, end_day)
        if other is not None:
            if sql.find('where') == -1:
                sql = "%s where %s" % (sql, other)
            else:
                sql = "%s and %s" % (sql, other)
        print(sql)
        result = self.cursor_interface.execute(sql)
        data = self.cursor_interface.fetchall()
        return data

    def get_stock_minute_date(self,
                              exchange,
                              minute_type,
                              year,
                              column_list=[],
                              code=None,
                              start=None,
                              end=None):
        if exchange is Exchange.sse:
            market_filter = 'market=\'212001\''
        if exchange is Exchange.szse:
            market_filter = 'market=\'212100\''
        if minute_type is MinuteType.one_minute:
            table_name = 'stock_basic_oneminute_' + year
        if minute_type is MinuteType.three_minute:
            table_name = 'stock_basic_threeminute_' + year
        if minute_type is MinuteType.five_minute:
            table_name = 'stock_basic_fiveminute_' + year
        if minute_type is MinuteType.fifteen_minute:
            table_name = 'stock_basic_fifteenminute_' + year
        if minute_type is MinuteType.thirty_minute:
            table_name = 'stock_basic_thirtyminute_' + year
        if minute_type is MinuteType.one_hour:
            table_name = 'stock_basic_onehour_' + year
        return self.get_minute_data(table_name, column_list, code, start, end, market_filter)

    def get_index_minute_date(self,
                              exchange,
                              minute_type,
                              year,
                              column_list=[],
                              code=None,
                              start=None,
                              end=None):
        market_filter = None
        if exchange is Exchange.cffex:
            if minute_type is MinuteType.one_minute:
                table_name = 'cffex_oneminute_' + year
            if minute_type is MinuteType.three_minute:
                table_name = 'cffex_threeminute_' + year
            if minute_type is MinuteType.five_minute:
                table_name = 'cffex_fiveminute_' + year
            if minute_type is MinuteType.fifteen_minute:
                table_name = 'cffex_fifteenminute_' + year
            if minute_type is MinuteType.thirty_minute:
                table_name = 'cffex_thirtyminute_' + year
            if minute_type is MinuteType.one_hour:
                table_name = 'cffex_onehour_' + year
        else:
            if exchange is Exchange.sse:
                market_filter = 'market=\'212001\''
            if exchange is Exchange.szse:
                market_filter = 'market=\'212100\''
            if minute_type is MinuteType.one_minute:
                table_name = 'index_basic_oneminute_' + year
            if minute_type is MinuteType.three_minute:
                table_name = 'index_basic_threeminute_' + year
            if minute_type is MinuteType.five_minute:
                table_name = 'index_basic_fiveminute_' + year
            if minute_type is MinuteType.fifteen_minute:
                table_name = 'index_basic_fifteenminute_' + year
            if minute_type is MinuteType.thirty_minute:
                table_name = 'index_basic_thirtyminute_' + year
            if minute_type is MinuteType.one_hour:
                table_name = 'index_basic_onehour_' + year

        return self.get_minute_data(table_name, column_list, code, start, end, market_filter)

    def get_tick_data(self,
                      table_name,
                      column_list=[],
                      code=None,
                      start=None,
                      end=None,
                      other=None):
        if len(column_list) == 0:
            columns = '*'
        else:
            columns = ','.join(column_list)
        if code is None and start is None and end is None:
            sql = "select %s from %s" % (columns, table_name)
            result = self.cursor_interface.execute(sql)
            data = self.cursor_interface.fetchall()
            return data
        sql = "select %s from %s" % (columns, table_name)
        if code is not None:
            if sql.find('where') == -1:
                sql = "%s where thscode=\'%s\'" % (sql, code)
            else:
                sql = "%s and thscode=\'%s\'" % (sql, code)
        if start is not None:
            if sql.find('where') == -1:
                sql = "%s where time_trade>=\'%s\'" % (sql, start)
            else:
                sql = "%s and time_trade>=\'%s\'" % (sql, start)
        if end is not None:
            if sql.find('where') == -1:
                sql = "%s where time_trade<=\'%s\'" % (sql, end)
            else:
                sql = "%s and time_trade<=\'%s\'" % (sql, end)
        if other is not None:
            if sql.find('where') == -1:
                sql = "%s where %s" % (sql, other)
            else:
                sql = "%s and %s" % (sql, other)
        print(sql)
        result = self.cursor_interface.execute(sql)
        data = self.cursor_interface.fetchall()
        return data

    def get_cffex_option_data(self,
                              date,
                              column_list=[],
                              code=None,
                              start=None,
                              end=None,
                              other=None):
        table_name = "cffex_option_tick_"
        time_year = date[0:4]
        time_day = date[4:]
        table_name = table_name + time_year
        table_name = table_name + '_'
        table_name = table_name + time_day
        return self.get_tick_data(table_name, column_list, code, start, end, other)

    def get_cffex_tick(self,
                       date,
                       column_list=[],
                       code=None,
                       start=None,
                       end=None,
                       other=None):
        table_name = "cffex_tick_"
        time_year = date[0:4]
        time_day = date[4:]
        table_name = table_name + time_year
        table_name = table_name + '_'
        table_name = table_name + time_day
        return self.get_tick_data(table_name, column_list, code, start, end, other)

    def get_sse_index_tick(self,
                           date,
                           column_list=[],
                           code=None,
                           start=None,
                           end=None,
                           other=None):
        table_name = "sh_index_now_"
        time_year = date[0:4]
        time_day = date[4:]
        table_name = table_name + time_year
        table_name = table_name + '_'
        table_name = table_name + time_day
        return self.get_tick_data(table_name, column_list, code, start, end, other)

    def get_sse_option_tick(self,
                            date,
                            column_list=[],
                            code=None,
                            start=None,
                            end=None,
                            other=None):
        table_name = "sh_option_tick_"
        time_year = date[0:4]
        time_day = date[4:]
        table_name = table_name + time_year
        table_name = table_name + '_'
        table_name = table_name + time_day
        return self.get_tick_data(table_name, column_list, code, start, end, other)

    def get_sse_stock_tick(self,
                           date,
                           column_list=[],
                           code=None,
                           start=None,
                           end=None,
                           other=None):
        table_name = "sh_stock_now_"
        time_year = date[0:4]
        time_day = date[4:]
        table_name = table_name + time_year
        table_name = table_name + '_'
        table_name = table_name + time_day
        return self.get_tick_data(table_name, column_list, code, start, end, other)

    def get_szse_index_tick(self,
                            date,
                            column_list=[],
                            code=None,
                            start=None,
                            end=None,
                            other=None):
        table_name = "sz_index_now_"
        time_year = date[0:4]
        time_day = date[4:]
        table_name = table_name + time_year
        table_name = table_name + '_'
        table_name = table_name + time_day
        return self.get_tick_data(table_name, column_list, code, start, end, other)

    def get_szse_option_tick(self,
                             date,
                             column_list=[],
                             code=None,
                             start=None,
                             end=None,
                             other=None):
        table_name = "sz_option_tick_"
        time_year = date[0:4]
        time_day = date[4:]
        table_name = table_name + time_year
        table_name = table_name + '_'
        table_name = table_name + time_day
        return self.get_tick_data(table_name, column_list, code, start, end, other)

    def get_szse_stock_tick(self,
                            date,
                            column_list=[],
                            code=None,
                            start=None,
                            end=None,
                            other=None):
        table_name = "sz_stock_now_"
        time_year = date[0:4]
        time_day = date[4:]
        table_name = table_name + time_year
        table_name = table_name + '_'
        table_name = table_name + time_day
        return self.get_tick_data(table_name, column_list, code, start, end, other)

    def get_stock_order(self,
                        date,
                        column_list=[],
                        code=None,
                        start=None,
                        end=None,
                        other=None):
        table_name = "stock_order_"
        time_year = date[0:4]
        time_day = date[4:]
        table_name = table_name + time_year
        table_name = table_name + '_'
        table_name = table_name + time_day
        if len(column_list) == 0:
            columns = '*'
        else:
            columns = ','.join(column_list)
        if code is None and start is None and end is None and other is None:
            sql = "select %s from %s" % (columns, table_name)
            result = self.cursor_interface.execute(sql)
            data = self.cursor_interface.fetchall()
            return data
        sql = "select %s from %s" % (columns, table_name)
        if code is not None:
            if sql.find('where') == -1:
                sql = "%s where thscode=\'%s\'" % (sql, code)
            else:
                sql = "%s and thscode=\'%s\'" % (sql, code)
        if start is not None:
            if sql.find('where') == -1:
                sql = "%s where time_trade>=\'%s\'" % (sql, start)
            else:
                sql = "%s and time_trade>=\'%s\'" % (sql, start)
        if end is not None:
            if sql.find('where') == -1:
                sql = "%s where time_trade<=\'%s\'" % (sql, end)
            else:
                sql = "%s and time_trade<=\'%s\'" % (sql, end)
        if other is not None:
            if sql.find('where') == -1:
                sql = "%s where %s" % (sql, other)
            else:
                sql = "%s and %s" % (sql, other)
        print(sql)
        result = self.cursor_interface.execute(sql)
        data = self.cursor_interface.fetchall()
        return data

    def get_stock_queue(self,
                        date,
                        column_list=[],
                        code=None,
                        start=None,
                        end=None,
                        other=None):
        table_name = "stock_queue_"
        time_year = date[0:4]
        time_day = date[4:]
        table_name = table_name + time_year
        table_name = table_name + '_'
        table_name = table_name + time_day
        if len(column_list) == 0:
            columns = '*'
        else:
            columns = ','.join(column_list)
        if code is None and start is None and end is None and other is None:
            sql = "select %s from %s" % (columns, table_name)
            result = self.cursor_interface.execute(sql)
            data = self.cursor_interface.fetchall()
            return data
        sql = "select %s from %s" % (columns, table_name)
        if code is not None:
            if sql.find('where') == -1:
                sql = "%s where thscode=\'%s\'" % (sql, code)
            else:
                sql = "%s and thscode=\'%s\'" % (sql, code)
        if start is not None:
            if sql.find('where') == -1:
                sql = "%s where time_trade>=\'%s\'" % (sql, start)
            else:
                sql = "%s and time_trade>=\'%s\'" % (sql, start)
        if end is not None:
            if sql.find('where') == -1:
                sql = "%s where time_trade<=\'%s\'" % (sql, end)
            else:
                sql = "%s and time_trade<=\'%s\'" % (sql, end)
        if other is not None:
            if sql.find('where') == -1:
                sql = "%s where %s" % (sql, other)
            else:
                sql = "%s and %s" % (sql, other)
        print(sql)
        result = self.cursor_interface.execute(sql)
        data = self.cursor_interface.fetchall()
        return data

    def get_stock_trade(self,
                        date,
                        column_list=[],
                        code=None,
                        start=None,
                        end=None,
                        other=None):
        table_name = "stock_trade_"
        time_year = date[0:4]
        time_day = date[4:]
        table_name = table_name + time_year
        table_name = table_name + '_'
        table_name = table_name + time_day
        if len(column_list) == 0:
            columns = '*'
        else:
            columns = ','.join(column_list)
        if code is None and start is None and end is None and other is None:
            sql = "select %s from %s" % (columns, table_name)
            result = self.cursor_interface.execute(sql)
            data = self.cursor_interface.fetchall()
            return data
        sql = "select %s from %s" % (columns, table_name)
        if code is not None:
            if sql.find('where') == -1:
                sql = "%s where thscode=\'%s\'" % (sql, code)
            else:
                sql = "%s and thscode=\'%s\'" % (sql, code)
        if start is not None:
            if sql.find('where') == -1:
                sql = "%s where time_trade>=\'%s\'" % (sql, start)
            else:
                sql = "%s and time_trade>=\'%s\'" % (sql, start)
        if end is not None:
            if sql.find('where') == -1:
                sql = "%s where time_trade<=\'%s\'" % (sql, end)
            else:
                sql = "%s and time_trade<=\'%s\'" % (sql, end)
        if other is not None:
            if sql.find('where') == -1:
                sql = "%s where %s" % (sql, other)
            else:
                sql = "%s and %s" % (sql, other)
        print(sql)
        result = self.cursor_interface.execute(sql)
        data = self.cursor_interface.fetchall()
        return data


def get_futures_data(code,
                    date,
                    column_list=[],
                    start=None,
                    end=None,
                    other=None):
    table_name = ''
    if date <= "20160930":
        table_name = r'//192.168.1.13/data/tick_hdf5/future_price_tradeday/' + date[0:6] + 'future_price_' + date + '.h5'
    elif date <= "20180323":
        table_name = '//192.168.1.13/data/tick_hdf5/future_price/' + date[0:6] + '/future_price_' + date + '.h5'
    else:
        table_name = '//192.168.1.13/data/tick_hdf5/ctp_tick/' + date[0:6] + '/ctp_tick_' + date + '.h5'

    data_other = pd.read_hdf(table_name, key=code)
    return data_other


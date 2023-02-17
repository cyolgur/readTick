import dataAdapter.mysqlAdapter as dataAdapter

if __name__ == '__main__':
    mysql = dataAdapter.sql_adapter('192.168.1.22', 'public', 'jz123456')
    mysql.connect('stock')
    # code 为secuCode字段 []为筛选的的列，空则显示全部， start为开始时间， end 为结束时间， 不填默认所有， table_name 为表名，默认为该表
    # data = mysql.get_stock_daily_data(column_list, code=None, start=None, end=None, table_name='stockdailydata')
    data = mysql.get_stock_daily_data([], code=1)  # 帅选secuCode 为1 的所有行
    print(len(data))
    # print(data)
    '''
    def get_stock_daily_history_data(self,
                           column_list,
                           code = None,
                           start = None,
                           end = None,
                           table_name = 'stockdailydatahistory'):
    与get_stock_daily_data 基本相同， 只不过默认表的名称不同
    '''
    data = mysql.get_stock_daily_history_data([], start='20050110', end='20050112')
    print(len(data))
    '''
    def get_stock_data(self,
                       column_list,
                       table_name = 'allstock',
                       secu_code = None,
                       secu_abbr = None,
                       market = None,
                       string_secu_code = None):
    column_list 为筛选出的列 table_name 默认allstock 
    '''
    # data = mysql.get_stock_data(['SecuCode'], secu_code='1')
    # print(data)
    # data = mysql.get_stock_data([], secu_code='1')
    # print(data)

    '''
    def get_index_component_weight_data(self,
                       column_list,
                       table_name = 'indexcomponentweight',
                       index_code = None,
                       secu_code = None,
                       secu_abbr = None,
                       trading_day = None,
                       industry = None):

    '''
    # data = mysql.get_index_component_weight_data([], index_code='399905')
    # print(data)
    print(len(data))

    '''
    def get_industry_classify_data(self,
                                   column_list,
                                   table_name = 'industryclassify',
                                   secu_code = None,
                                   trading_day = None,
                                   zx_first_industry = None,
                                   start = None,
                                   end = None):
    '''
    # data = mysql.get_industry_classify_data([], zx_first_industry='银行')
    # print(data)
    print(len(data))

    '''
    def get_stock_share_data(self,
                             column_list,
                             secu_code = None,
                             trading_day = None,
                             start = None,
                             end = None,
                             table_name = 'stockshare'):
    '''
    data = mysql.get_stock_share_data([], start='20150101', end='20150105')
    print(data)
    print(len(data))
    # mysql.close()
    # mysql.re_connect(table_name) 可以重新连接其他数据库
    # print(type(data))

    '''
    导入枚举类型
    包含了交易所类型 数据分钟类型
    class MinuteType(Enum):
        one_minute = 0          分钟数据
        three_minute = 1        三分钟数据
        five_minute = 2         五分钟数据
        ten_minute = 3          十分钟数据
        fifteen_minute = 4      十五分钟数据
        thirty_minute = 5       三十分钟数据
        one_hour = 6            六十分钟数据

    class Exchange(Enum):
        sse = 0                 上交所
        szse = 1                深交所
        bjse = 2                北交所
        shfe = 3                上期所
        cffex = 4               中金所
        dce = 5                 大商所
        zce = 6                 郑商所
    '''
    from dataAdapter import Exchange as exc
    from dataAdapter import MinuteType as minute


    '''
    重新连接新的数据库，‘ifind’为数据库名，‘latin1’为数据库字符集，默认值为‘utf8mb4’，不同字符集要自行输入
    否则会导致读取失败或者乱码
    '''
    mysql.re_connect("ifind", 'latin1')

    '''
    def get_stock_minute_date(self,
                               exchange,
                               minute_type,
                               year,
                               column_list = [],
                               code = None,
                               start = None,
                               end = None)
    exchange 为交易所枚举类型，使用导入的枚举类型
    minute_type 为数据分钟类型，使用导入的时间枚举
    year 为查询的时间（年限），必须填写，因为数据库表的名字后以年限作为区分
    column_list 作为要筛选的内容，不填则为所有栏目
    start 开始时间
    end 结束时间
    '''
    data = mysql.get_stock_minute_date(exc.sse, minute.one_minute, '2020', code='601028.SH', start='20201208',
                                       end='20201209')
    print(data)
    print(len(data))

    '''
        def get_stock_minute_date(self,
                               exchange,
                               minute_type,
                               year,
                               column_list = [],
                               code = None,
                               start = None,
                               end = None):
        exchange 为交易所枚举类型，使用导入的枚举类型
        minute_type 为数据分钟类型，使用导入的时间枚举
        year 为查询的时间（年限），必须填写，因为数据库表的名字后以年限作为区分
        column_list 作为要筛选的内容，不填则为所有栏目
        start 开始时间
        end 结束时间
    '''
    data = mysql.get_stock_minute_date(exc.sse, minute.one_minute, '2021', column_list=['thscode'], code='601028.SH',
                                       start='20211208',
                                       end='20211209')
    print(data)
    print(len(data))

    '''
    def get_index_minute_date(self,
                               exchange,
                               minute_type,
                               year,
                               column_list = [],
                               code = None,
                               start = None,
                               end = None)
    exchange 为交易所枚举类型，使用导入的枚举类型
    minute_type 为数据分钟类型，使用导入的时间枚举
    year 为查询的时间（年限），必须填写，因为数据库表的名字后以年限作为区分
    column_list 作为要筛选的内容，不填则为所有栏目
    start 开始时间
    end 结束时间
    '''
    data = mysql.get_index_minute_date(exc.cffex, minute.fifteen_minute, '2021', column_list=['thscode'],
                                       code='IC2102.CFE',
                                       start='20210104',
                                       end='20210105')
    print(data)
    print(len(data))

    '''
        def get_index_minute_date(self,
                               exchange,
                               minute_type,
                               year,
                               column_list = [],
                               code = None,
                               start = None,
                               end = None)
        exchange 为交易所枚举类型，使用导入的枚举类型
        minute_type 为数据分钟类型，使用导入的时间枚举
        year 为查询的时间（年限），必须填写，因为数据库表的名字后以年限作为区分
        column_list 作为要筛选的内容，不填则为所有栏目
        start 开始时间
        end 结束时间
    '''
    data = mysql.get_index_minute_date(exc.sse, minute.fifteen_minute, '2021', column_list=['thscode'],
                                       code='000038.SH',
                                       start='20210104',
                                       end='20210105')
    print(data)
    print(len(data))

    '''
    def get_cffex_tick(self,
                       date,
                       column_list = [],
                       code = None,
                       start = None,
                       end = None,
                       other = None)
    date 为查询日期
    column_list 作为要筛选的内容，不填则为所有栏目
    start 为查询时间起点 格式 XX:XX:XX 小时：分钟：秒数
    end 为查询结束时间 格式与时间起点相同
    code 合约代码
    other 为 查询的其他条件 例如 'ms>200' 多个条件以 'and' 连接 语法与sql 一致
    '''
    data = mysql.get_cffex_tick('20210811', column_list=['thscode'], code='IC2203.CFE', start='00:00:00', end='15:00:00')
    print(data)
    print(len(data))

    '''
    def get_sse_index_tick(self,
                       date,
                       column_list = [],
                       code = None,
                       start = None,
                       end = None,
                       other = None)
    获取上证指数tick
    输入参数与上面一致
    '''
    data = mysql.get_sse_index_tick('20210630', column_list=['thscode'], code='000046.SH', start='00:00:00', end='15:00:00')
    print(data)
    print(len(data))

    '''
    def get_sse_option_tick(self,
                       date,
                       column_list = [],
                       code = None,
                       start = None,
                       end = None,
                       other = None)
    获取上证期权tick、
    输入参数与上面一致
    '''
    data = mysql.get_sse_option_tick('20211022', column_list=['thscode'], code='10003397.SH', start='00:00:00',
                                    end='15:00:00')
    print(data)
    print(len(data))

    '''
     def get_sse_stock_tick(self,
                       date,
                       column_list = [],
                       code = None,
                       start = None,
                       end = None,
                       other = None)
    获取上证股票tick
    输入参数与上面一致
    '''
    data = mysql.get_sse_stock_tick('20210721', column_list=[], code='600823.SH', start='00:00:00',
                                     end='15:00:00')
    print(data)
    print(len(data))

    '''
    def get_szse_index_tick(self,
                           date,
                           column_list=[],
                           code=None,
                           start=None,
                           end=None,
                           other=None)
    获取深证指数tick
    输入参数与上面一致
    '''
    data = mysql.get_szse_index_tick('20210630', column_list=[], code='399688.SZ', start='00:00:00',
                                    end='15:00:00')
    print(data)
    print(len(data))

    '''
    def get_szse_option_tick(self,
                           date,
                           column_list=[],
                           code=None,
                           start=None,
                           end=None,
                           other=None)
    获取深交所期权tick
    输入参数与上面一致
    '''
    data = mysql.get_szse_option_tick('20210809', column_list=[], code='90000600.SZ', start='00:00:00',
                                     end='15:00:00')
    print(data)
    print(len(data))

    '''
    def get_szse_stock_tick(self,
                           date,
                           column_list=[],
                           code=None,
                           start=None,
                           end=None,
                           other=None)
    获取深交所股票tick
    输入参数与上面一致
    '''
    data = mysql.get_szse_stock_tick('20210702', column_list=[], code='000998.SZ', start='00:00:00',
                                      end='15:00:00')
    print(data)
    print(len(data))

    '''
    def get_stock_order(self,
                       date,
                       column_list=[],
                       code=None,
                       start=None,
                       end=None,
                       other=None)
    获取股票报单
    输入参数与上面一致
    '''
    data = mysql.get_stock_order('20211104', column_list=[], code='002952.SZ', start='00:00:00.000',
                                     end='15:00:00.000')
    print(data)
    print(len(data))

    '''
    def get_stock_queue(self,
                       date,
                       column_list=[],
                       code=None,
                       start=None,
                       end=None,
                       other=None)
    获取股票买卖队列
    输入参数与上面一致
    '''
    data = mysql.get_stock_queue('20210803', column_list=[], code='601028.SH', start='00:00:00',
                                 end='15:00:00')
    print(data)
    print(len(data))

    '''
    def get_stock_trade(self,
                       date,
                       column_list=[],
                       code=None,
                       start=None,
                       end=None,
                       other=None)
    获取股票逐笔成交
    输入参数与上面一致
    '''
    data = mysql.get_stock_trade('20210730', column_list=[], code='601028.SH', start='00:00:00',
                                 end='15:00:00')
    print(data)
    print(len(data))

    '''
        get_futures_data(code,
                    date,
                    column_list=[],
                    start=None,
                    end=None,
                    other=None)
        获取tick数据
        '''
    data = dataAdapter.get_futures_data('A2111', '20210917')
    print(data)

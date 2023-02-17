import dataAdapter.mysqlAdapter as dataAdapter
mysql = dataAdapter.sql_adapter('192.168.1.22', 'public', 'jz123456')
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

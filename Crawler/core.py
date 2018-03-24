__author__ = 'phlai'
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import os
from .util import str2datetime

class StockData:
    def __init__(self,
                 tse_file='../../Charm/StockInfo/',
                 otc_file='../../Charm/StockInfo_otc/',
                 ind_file='../../Charm/IndexData',
                 pay_file='../../Charm/IndexPayData'):
        #  read db_register
        self.tse_db_register = pd.read_csv(
            tse_file + 'DB_Register.csv',
            dtype={'StockCode': str, 'DB_Idx': int})
        self.tse_db_register.set_index('StockCode', inplace=True)

        self.otc_db_register = pd.read_csv(
            otc_file + 'DB_Register.csv',
            dtype={'StockCode': str, 'DB_Idx': int})
        self.otc_db_register.set_index('StockCode', inplace=True)

        #  read insti3 register
        self.tse_insti3_register = pd.read_csv(
            tse_file + 'Insti3_Register.csv',
            dtype={'StockCode': str, 'DB_Idx': int})
        self.tse_insti3_register.set_index('StockCode', inplace=True)

        self.otc_insti3_register = pd.read_csv(
            otc_file + 'Insti3_Register.csv',
            dtype={'StockCode': str, 'DB_Idx': int})
        self.otc_insti3_register.set_index('StockCode', inplace=True)

        #  read margin balance register
        self.tse_mb_register = pd.read_csv(tse_file + 'MB_Register.csv',
                                           dtype={'StockCode': str, 'DB_Idx': int})
        self.tse_mb_register.set_index('StockCode', inplace=True)

        self.otc_mb_register = pd.read_csv(otc_file + 'MB_Register.csv',
                                           dtype={'StockCode': str, 'DB_Idx': int})
        self.otc_mb_register.set_index('StockCode', inplace=True)

        #  create price engines
        self.tse_db_engines = {}
        for idx in self.tse_db_register.DB_Idx.unique():
            self.tse_db_engines[idx] = create_engine('sqlite:///%sDB_%d.db' % (tse_file, idx), echo=False)

        self.otc_db_engines = {}
        for idx in self.otc_db_register.DB_Idx.unique():
            self.otc_db_engines[idx] = create_engine('sqlite:///%sDB_%d.db' % (otc_file, idx), echo=False)

        #  create insti3 engines
        self.tse_insti3_engines = {}
        for idx in self.tse_insti3_register.DB_Idx.unique():
            self.tse_insti3_engines[idx] = create_engine('sqlite:///%sInsti3_%d.db' % (tse_file, idx), echo=False)

        self.otc_insti3_engines = {}
        for idx in self.otc_insti3_register.DB_Idx.unique():
            self.otc_insti3_engines[idx] = create_engine('sqlite:///%sInsti3_%d.db' % (otc_file, idx), echo=False)

        #  create margin balance engines
        self.tse_mb_engines = {}
        for idx in self.tse_mb_register.DB_Idx.unique():
            self.tse_mb_engines[idx] = create_engine('sqlite:///%sMB_%d.db' % (tse_file, idx), echo=False)

        self.otc_mb_engines = {}
        for idx in self.otc_mb_register.DB_Idx.unique():
            self.otc_mb_engines[idx] = create_engine('sqlite:///%sMB_%d.db' % (otc_file, idx), echo=False)

        #  Get indice data
        self.ind_file = ind_file
        data_list = []
        now_time = datetime.now()
        for year in range(2003, now_time.year + 1):
            file_name = '%s/%d.csv' % (self.ind_file, year)
            if os.path.isfile(file_name):
                df = pd.read_csv(file_name, dtype={'date': str})
                df['date'] = df['date'].apply(str2datetime)
                data_list.append(df)
        self.ind_data = pd.concat(data_list)

        self.pay_file = pay_file
        data_list = []
        now_time = datetime.now()
        for year in range(2003, now_time.year + 1):
            file_name = '%s/%d.csv' % (self.ind_file, year)
            if os.path.isfile(file_name):
                df = pd.read_csv(file_name, dtype={'date': str})
                df['date'] = df['date'].apply(str2datetime)
                data_list.append(df)
        self.pay_data = pd.concat(data_list)

    def check_data_file(self, stock_code):
        #  -1: no data ; 1: in tse ; 2: in otc ; 3: both
        if stock_code in self.tse_db_register.index:
            if stock_code in self.otc_db_register.index:
                return 3
            else:
                return 1
        elif stock_code in self.otc_db_register.index:
            return 2
        else:
            return -1

    def get_price(self, stock_code):
        data_file = self.check_data_file(stock_code)

        if data_file == -1:
            return -1
        df1 = df2 = -1
        if (data_file == 1) or (data_file == 3):
            idx = self.tse_db_register.loc[stock_code, 'DB_Idx']
            df1 = pd.read_sql(stock_code, con=self.tse_db_engines[idx])
        if data_file >= 2:
            idx = self.otc_db_register.loc[stock_code, 'DB_Idx']
            df2 = pd.read_sql(stock_code, con=self.otc_db_engines[idx])
            #  DONE: fix format
            #  volume = 成交量(股)
            #  transaction = 成交筆數 
            #  turnover = 總成交金額
            #  UD = 上下符號
            #  difference = 漲跌價差
            #  PE_ratio = 本益比
            otc2tse_price_col = ['code', 'name', 'volume', 'transaction',
                                 'turnover', 'open', 'high', 'low', 'close', 'UD',
                                 'difference', 'last_buy', 'last_buy_volume',
                                 'last_sell', 'last_sell_volume', 'PE_ratio', 'date']
            df2['UD'] = df2['difference'].apply(lambda x: ' ' if x.startswith('0') else x[0])
            df2['difference'] = df2['difference'].apply(lambda x: x if x.startswith('0') else x[1:])
            df2['last_buy_volume'] = ''
            df2['last_sell_volume'] = ''
            df2['PE_ratio'] = ''
            df2 = df2[otc2tse_price_col]

        if data_file == 1:
            return df1
        elif data_file == 2:
            return df2
        else:
            df = pd.concat([df1, df2])
            df.sort_index(inplace=True)
            return df

    def get_insti3(self, stock_code):
        #  -1: no data ; 1: in tse ; 2: in otc ; 3: both
        if stock_code in self.tse_insti3_register.index:
            if stock_code in self.otc_insti3_register.index:
                data_file = 3
            else:
                data_file = 1
        elif stock_code in self.otc_insti3_register.index:
            data_file = 2
        else:
            return -1

        df1 = -1
        df2 = -1
        if (data_file == 1) or (data_file == 3):
            idx = self.tse_insti3_register.loc[stock_code, 'DB_Idx']
            df1 = pd.read_sql(stock_code, con=self.tse_insti3_engines[idx])

            # fix format
            df1 = df1.rename(columns={'D_證券名稱': '證券名稱',
                                      'D_買賣超股數': '自營買賣', 'D_買賣超股數(自行)': '自營買賣(自行)',
                                      'D_買賣超股數(避險)': '自營買賣(避險)', 'D_買進股數': '自營買',
                                      'D_買進股數(自行)': '自營買(自行)', 'D_買進股數(避險)': '自營買(避險)',
                                      'D_賣出股數': '自營賣', 'D_賣出股數(自行)': '自營賣(自行)',
                                      'D_賣出股數(避險)': '自營賣(避險)', 'F_證券名稱': '外資證券名稱',
                                      'F_買賣超股數': '外資買賣', 'F_買進股數': '外資買',
                                      'F_賣出股數': '外資賣', 'I_證券名稱': '投信證券名稱',
                                      'I_買賣超股數': '投信買賣', 'I_買進股數': '投信買',
                                      'I_賣出股數': '投信賣'})

            df1['三大買賣'] = df1['自營買賣'].astype(int) + df1['外資買賣'].astype(int) + df1['投信買賣'].astype(int)
            df1 = df1[['code', '證券名稱', '外資買', '外資賣',
                       '外資買賣', '投信買', '投信賣', '投信買賣',
                       '自營買(自行)', '自營賣(自行)', '自營買賣(自行)',
                       '自營買(避險)', '自營賣(避險)', '自營買賣(避險)',
                       '自營買', '自營賣', '自營買賣', '三大買賣', 'date']]

        if data_file >= 2:
            idx = self.otc_insti3_register.loc[stock_code, 'DB_Idx']
            df2 = pd.read_sql(stock_code, con=self.otc_insti3_engines[idx])

        if data_file == 1:
            return df1
        elif data_file == 2:
            return df2
        else:
            df = pd.concat([df1, df2])
            df.sort_index(inplace=True)
            return df

    def get_mb(self, stock_code):

        if stock_code in self.tse_mb_register.index:
            if stock_code in self.otc_mb_register.index:
                data_file = 3
            else:
                data_file = 1
        elif stock_code in self.otc_mb_register.index:
            data_file = 2
        else:
            return -1

        # data_file = self.check_data_file(stock_code)

        # if data_file == -1:
        #     return -1
        df1 = df2 = -1
        final_mb_col = ['code', 'name', '資買', '資賣', '現償',
                        '前資餘額', '資餘額', '資限額', '資使用率(%)',
                        '資屬證金', '券買', '券賣', '券償', '前券餘額',
                        '券餘額', '券限額', '券使用率(%)', '券屬證金',
                        '資券相抵', '備註', 'date']

        if (data_file == 1) or (data_file == 3):
            idx = self.tse_mb_register.loc[stock_code, 'DB_Idx']
            df1 = pd.read_sql(stock_code, con=self.tse_mb_engines[idx])

            df1 = df1.rename(columns={'股票名稱': 'name', '融資買進': '資買', '融資賣出': '資賣',
                                      '融資現金償還': '現償', '融資前日餘額': '前資餘額', '融資今日餘額': '資餘額',
                                      '融資限額': '資限額',
                                      '融券買進': '券買', '融券賣出': '券賣', '融券現金償還': '券償',
                                      '融券前日餘額': '前券餘額', '融券今日餘額': '券餘額', '融券限額': '券限額',
                                      '資券互抵': '資券相抵', '註記': '備註'})
            df1['資使用率(%)'] = (df1['資餘額'].astype(float)/df1['資限額'].astype(float))*100
            df1['券使用率(%)'] = (df1['券餘額'].astype(float)/df1['券限額'].astype(float))*100
            df1['資屬證金'] = -1
            df1['券屬證金'] = -1
            df1 = df1[final_mb_col]
        if data_file >= 2:
            idx = self.otc_mb_register.loc[stock_code, 'DB_Idx']
            df2 = pd.read_sql(stock_code, con=self.otc_mb_engines[idx])
            #  DONE: fix format
            df2 = df2.rename(columns={'前資餘額(張)': '前資餘額',
                                      '前券餘額(張)': '前券餘額',
                                      '資券相抵(張)': '資券相抵'})
            df2 = df2[final_mb_col]

        if data_file == 1:
            return df1
        elif data_file == 2:
            return df2
        else:
            df = pd.concat([df1, df2])
            df.sort_index(inplace=True)
            return df

    # def get_actions(self, stock_code):

    # def get_action(self, stock_code):
        #  TO DO

        # data_file = check_data_file(stock_code)
        # if data_file == 1:
        #     idx = self.tse_db_register.loc[stock_code, 'DB_Idx']
        #     df1 = pd.read_sql(stock_code, con=self.tse_mb_engines[idx])
        #     return df1
        # if data_file >= 2:
        #     idx = self.otc_mb_register.loc[stock_code, 'DB_Idx']
        #     df2 = pd.read_sql(stock_code, con=self.otc_mb_engines[idx])

        # if self.db_idx == -1:
        #     self.action = -1
        #     return -1
        # file_name = self.file_list[self.data_file_loc]
        # engine = create_engine(
        #     'sqlite:///%s/Act_%d.db' % (file_name, self.db_idx),
        #     echo=False)
        # self.action = pd.read_sql(self.stock_code, con=engine)

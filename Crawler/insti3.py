__author__ = 'phlai'

import pandas as pd
import numpy as np
from pandas import DataFrame

# from pandas_datareader import data
from datetime import datetime, timedelta

from sqlalchemy import create_engine

import requests
import urllib

import re
import os
import logging
import time

from .util import MyError

from .grepper import get_merge_inv3
from .grepper import get_otc_insti3
#  三大法人(機構投資人) Institutional investors
#  買賣超 Net Buy / Net Ssell
#  散戶 Retail Investors
#  大盤指數 (TSE) index/ TAIEX
#  外資 Foreign Investor(s) / Foreign Investment Institution
#  投信 Investment Trust / Domestic Institution (用於與外資作區別時)
#  自營商 Dealer


class UpdateInsti3:

    def __init__(self, tse_file='tse', otc_file='otc'):
        if not os.path.isdir(tse_file):
            print('TSE file not exist')
            raise MyError(-1)
        self.tse_file = tse_file

        if not os.path.isdir(otc_file):
            print('OTC file not exist')
            raise MyError(-1)
        self.otc_file = otc_file

        # Build DB_Register
        self.DB_Register_Addr = 0
        self.DB_Register = DataFrame()
        self.engines = {}

        self.working_file = ''

    def build_engines(self):
        self.engines = {}

        for DB_idx in self.DB_Register['DB_Idx'].unique():
            if not np.isnan(DB_idx):
                self.engines[DB_idx] = create_engine('sqlite:///%s/Insti3_%d.db' %
                                                     (self.working_file, DB_idx),
                                                     echo=False)

    def save_register(self):
        # print(self.DB_Register.head())
        # self.DB_Register.to_csv(self.DB_Register_Addr)

        self.DB_Register.reset_index(inplace=True)
        self.DB_Register.to_csv(self.DB_Register_Addr, index=False)
        self.DB_Register.set_index('StockCode', inplace=True)

    def save_df_by_name(self, table_name, df, db_idx):
        # print('table_name=%s, db_idx=%d' % (table_name, db_idx))
        # print(df.head())
        # print('table_name=%s, db_idx=%d' % (table_name, db_idx))
        # print(df.head())
        # if self.engines[db_idx].has_table(table_name):
        #     print(pd.read_sql(table_name, con=self.engines[db_idx]).head())
        # else:
        #     print('New table')

        df.to_sql(name=table_name, con=self.engines[db_idx], if_exists='append', index=False)

    def save_df_to_db(self, table_name, df):

        if table_name not in self.DB_Register.index:

            max_db_idx = self.DB_Register.DB_Idx.max()
            if np.isnan(max_db_idx):
                max_db_idx = 0
                self.engines[max_db_idx] = \
                    create_engine('sqlite:///%s/Insti3_%d.db' % (self.working_file, max_db_idx),
                                  echo=False)
            if len(self.engines[max_db_idx].table_names()) >= 500:
                max_db_idx += 1
                self.engines[max_db_idx] = \
                    create_engine('sqlite:///%s/Insti3_%d.db' % (self.working_file, max_db_idx),
                                  echo=False)
            self.DB_Register.loc[table_name, 'DB_Idx'] = max_db_idx

            # self.save_register()

        self.save_df_by_name(table_name, df, self.DB_Register.DB_Idx[table_name])

    def save_all_data(self, save_list, date_str, check_day_path):
        save_df = pd.concat(save_list)
        save_df.fillna(0, inplace=True)
        # print(save_df.head())
        gp = save_df.groupby('code')
        for code, df in gp:
            proc = df.copy()
            proc.sort_values('date', inplace=True)
            # if isinstance(code, int):
            #     save_df.to_excel('Error1.xlsx')
            #     proc.to_excel('Error2.xlsx')
            #     raise MyError(-2)
            self.save_df_to_db(code, proc)

        self.save_register()
        with open(check_day_path, 'w', encoding='UTF-8') as file:
            file.write(date_str)

    def check_all_data(self):
        date_diff = timedelta(days=1)

        #  Now Day
        now_time = datetime.now()
        now_date = datetime(now_time.year, now_time.month, now_time.day)

        collect_func = get_merge_inv3

        for working_file in [self.tse_file, self.otc_file]:
            self.working_file = working_file
            check_day_path = self.working_file + '/CheckDay_insti3.txt'

            #  Start Day
            if os.path.isfile(check_day_path):
                with open(check_day_path, 'r', encoding='UTF-8') as file:
                    check_day_str = file.read()
                start_year = int(check_day_str[0:4])
                start_month = int(check_day_str[4:6])
                start_day = int(check_day_str[6:])
                start_date = datetime(start_year, start_month, start_day)
                start_date += date_diff
            else:
                if working_file == self.tse_file:
                    #  TSE 3 Institutional investors data
                    #  Start from 2004/12/17
                    start_date = datetime(2004, 12, 17)
                else:
                    #  OTC 3 Institutional investors data
                    #  Start from 2004/06/01
                    start_date = datetime(2004, 6, 1)

            # Build DB_Register
            self.DB_Register_Addr = working_file + '/Insti3_Register.csv'
            if not os.path.isfile(self.DB_Register_Addr):
                self.DB_Register = DataFrame(columns=['DB_Idx'])
                self.DB_Register.index.name = 'StockCode'
            else:
                self.DB_Register = pd.read_csv(self.DB_Register_Addr,
                                               dtype={'StockCode': str, 'DB_Idx': int})
                self.DB_Register.set_index('StockCode', inplace=True)

            self.build_engines()

            #  Start Loop
            last_success_day = 0
            save_list = []
            while start_date <= now_date:

                #  Print Log
                date_str = '{0}{1:02d}{2:02d}'.\
                    format(start_date.year, start_date.month, start_date.day)
                print('Read ' + date_str + ' ' + working_file + ' Insti3')

                # time.sleep(np.random.random())

                data_df = collect_func(start_date)
                time.sleep(np.random.random() + 3)

                if isinstance(data_df, DataFrame):
                    #  success

                    #  only leave stock, stock code len == 4
                    # data_df = data_df[data_df.code.str.len() == 4]

                    save_list.append(data_df)
                    if len(save_list) >= 60:
                        print('Save')
                        self.save_all_data(save_list, date_str, check_day_path)
                        save_list = []
                    last_success_day = date_str
                else:
                    #  is an off day
                    print(date_str + ' is an offday')

                start_date += date_diff

            if save_list:
                print('Last Save')
                self.save_all_data(save_list, last_success_day, check_day_path)

            collect_func = get_otc_insti3

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

from .grepper import get_tse_margin_balance, get_otc_margin_balance

_filter_len = 4


class UpdateMarginBalance:
    def __init__(self, prefix='data'):
        if not os.path.isdir(prefix):
            os.mkdir(prefix)
        self.prefix = prefix

        # Build DB_Register
        self.DB_Register_Addr = prefix + '/MB_Register.csv'
        if not os.path.isfile(self.DB_Register_Addr):
            self.DB_Register = DataFrame(columns=['DB_Idx'])
            self.DB_Register.index.name = 'StockCode'
        else:
            self.DB_Register = pd.read_csv(self.DB_Register_Addr,
                                           dtype={'StockCode': str, 'DB_Idx': int})
            self.DB_Register.set_index('StockCode', inplace=True)

        # Build Engine
        self.engines = {}
        for DB_idx in self.DB_Register['DB_Idx'].unique():
            self.engines[DB_idx] = create_engine('sqlite:///%s/MB_%d.db' % (self.prefix, DB_idx), echo=False)

    def return_file(self):
        return self.prefix

    def save_register(self):
        # self.DB_Register.reset_index(inplace=True).0
        # self.DB_Register.to_csv(self.DB_Register_Addr)
        # self.DB_Register.set_index('StockCode', inplace=True)
        self.DB_Register.reset_index(inplace=True)
        self.DB_Register.to_csv(self.DB_Register_Addr, index=False)
        self.DB_Register.set_index('StockCode', inplace=True)

    def save_df_by_name(self, table_name, df, db_idx):

        # engine = create_engine('sqlite:///' + DB_str, echo=False)
        df.to_sql(name=table_name, con=self.engines[db_idx], if_exists='append', index=False)

    def save_df_to_db(self, table_name, df):

        # Check Table exist or not
        if table_name not in self.DB_Register.index:

            # choose the latest DB
            max_db_idx = self.DB_Register.DB_Idx.max()
            if np.isnan(max_db_idx):
                max_db_idx = 0
                self.engines[max_db_idx] = \
                    create_engine('sqlite:///%s/MB_%d.db' % (self.prefix, max_db_idx),
                                  echo=False)

            # if the latest DB's Table number is 500
            if len(self.engines[max_db_idx].table_names()) >= 500:
                # make a new DB
                max_db_idx += 1
                self.engines[max_db_idx] = \
                    create_engine('sqlite:///%s/MB_%d.db' % (self.prefix, max_db_idx),
                                  echo=False)

            # self.DB_Register.DB_Idx[table_name] = max_db_idx
            self.DB_Register.loc[table_name, 'DB_Idx'] = max_db_idx

            self.save_register()

        self.save_df_by_name(table_name, df, self.DB_Register.DB_Idx[table_name])

    def save_all_data(self, save_list, date_str, check_day_path):
        save_df = pd.concat(save_list)
        gp = save_df.groupby('code')
        for code, df in gp:
            proc = df.copy()
            proc.sort_values('date', inplace=True)
            self.save_df_to_db(code, proc)

        #  Update check day
        with open(check_day_path, 'w', encoding='UTF-8') as file:
            file.write(date_str)

    def check_all_data(self):
        #  Start from 2001(90)/1/2 ~ present
        date_diff = timedelta(days=1)

        #  Start Day
        #  20031202
        #  20031222
        check_day_path = self.prefix + '/CheckDay_MB.txt'
        if os.path.isfile(check_day_path):
            with open(check_day_path, 'r', encoding='UTF-8') as file:
                check_day_str = file.read()
            start_year = int(check_day_str[0:4])
            start_month = int(check_day_str[4:6])
            start_day = int(check_day_str[6:])
            start_date = datetime(start_year, start_month, start_day)
            start_date += date_diff
        else:
            start_date = datetime(2001, 1, 2)

        #  Now Day
        now_time = datetime.now()
        now_date = datetime(now_time.year, now_time.month, now_time.day)

        #  Start Loop
        last_success_day = 0
        save_list = []
        while start_date <= now_date:

            #  Print Log
            date_str = '{0}{1:02d}{2:02d}'.\
                format(start_date.year, start_date.month, start_date.day)
            print('Read ' + date_str + ' Margin Balance TSC')

            # time.sleep(np.random.random()*10)
            time.sleep(np.random.random() + 3)

            data_df = get_tse_margin_balance(start_date)

            if isinstance(data_df, DataFrame):
                #  success
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


class UpdateMarginBalanceOTC:
    def __init__(self, prefix='data_otc'):
        if not os.path.isdir(prefix):
            os.mkdir(prefix)
        self.prefix = prefix

        # Build DB_Register
        self.DB_Register_Addr = prefix + '/MB_Register.csv'
        if not os.path.isfile(self.DB_Register_Addr):
            self.DB_Register = DataFrame(columns=['DB_Idx'])
            self.DB_Register.index.name = 'StockCode'
        else:
            self.DB_Register = pd.read_csv(self.DB_Register_Addr,
                                           dtype={'StockCode': str, 'DB_Idx': int})
            self.DB_Register.set_index('StockCode', inplace=True)

        # Build Engine
        self.engines = {}
        for DB_idx in self.DB_Register['DB_Idx'].unique():
            self.engines[DB_idx] = create_engine('sqlite:///%s/MB_%d.db' % (self.prefix, DB_idx), echo=False)

    def return_file(self):
        return self.prefix

    def save_register(self):
        # self.DB_Register.reset_index(inplace=True).0
        # self.DB_Register.to_csv(self.DB_Register_Addr)
        # self.DB_Register.set_index('StockCode', inplace=True)
        self.DB_Register.reset_index(inplace=True)
        self.DB_Register.to_csv(self.DB_Register_Addr, index=False)
        self.DB_Register.set_index('StockCode', inplace=True)

    def save_df_by_name(self, table_name, df, db_idx):

        # engine = create_engine('sqlite:///' + DB_str, echo=False)
        df.to_sql(name=table_name, con=self.engines[db_idx], if_exists='append', index=False)

    def save_df_to_db(self, table_name, df):

        # Check Table exist or not
        if table_name not in self.DB_Register.index:

            # choose the latest DB
            max_db_idx = self.DB_Register.DB_Idx.max()
            if np.isnan(max_db_idx):
                max_db_idx = 0
                self.engines[max_db_idx] = \
                    create_engine('sqlite:///%s/MB_%d.db' % (self.prefix, max_db_idx),
                                  echo=False)

            # if the latest DB's Table number is 500
            if len(self.engines[max_db_idx].table_names()) >= 500:
                # make a new DB
                max_db_idx += 1
                self.engines[max_db_idx] = \
                    create_engine('sqlite:///%s/MB_%d.db' % (self.prefix, max_db_idx),
                                  echo=False)

            # self.DB_Register.DB_Idx[table_name] = max_db_idx
            self.DB_Register.loc[table_name, 'DB_Idx'] = max_db_idx

            self.save_register()

        self.save_df_by_name(table_name, df, self.DB_Register.DB_Idx[table_name])

    def save_all_data(self, save_list, date_str, check_day_path):
        save_df = pd.concat(save_list)
        gp = save_df.groupby('code')
        for code, df in gp:
            proc = df.copy()
            proc.sort_values('date', inplace=True)
            self.save_df_to_db(code, proc)

        #  Update check day
        with open(check_day_path, 'w', encoding='UTF-8') as file:
            file.write(date_str)

    def check_all_data(self):
        #  Start from 2007(96)/1/2 ~ present
        date_diff = timedelta(days=1)

        #  Start Day
        check_day_path = self.prefix + '/CheckDay_MB.txt'
        if os.path.isfile(check_day_path):
            with open(check_day_path, 'r', encoding='UTF-8') as file:
                check_day_str = file.read()
            start_year = int(check_day_str[0:4])
            start_month = int(check_day_str[4:6])
            start_day = int(check_day_str[6:])
            start_date = datetime(start_year, start_month, start_day)
            start_date += date_diff
        else:
            start_date = datetime(2007, 1, 2)

        #  Now Day
        now_time = datetime.now()
        now_date = datetime(now_time.year, now_time.month, now_time.day)

        #  Start Loop
        last_success_day = 0
        save_list = []
        while start_date <= now_date:

            #  Print Log
            date_str = '{0}{1:02d}{2:02d}'.\
                format(start_date.year, start_date.month, start_date.day)
            print('Read ' + date_str + 'Margin Balance OTC')

            data_df = get_otc_margin_balance(start_date)

            if isinstance(data_df, DataFrame):
                #  success
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

__author__ = 'phlai'


# import pandas as pd
# import numpy as np
# from pandas import DataFrame

# from pandas_datareader import data
from datetime import timedelta

from sqlalchemy import create_engine

# import requests
# import urllib

# import re
import os
# import requests
# import time

# import logging

from .grepper import *

from .util import MyError, str2datetime

tse_price_config = {
    'file_name': 'StockInfo',
    'register_file': 'DB_Register',
    'check_date_file': 'CheckDay',
    'grepper_function': get_tse_one_day,
    'db_prefix': 'DB_',
    'first_date': '20040211'
}


tse_insti3_config = {
    'file_name': 'StockInfo',
    'register_file': 'Insti3_Register',
    'check_date_file': 'CheckDay_insti3',
    'grepper_function': get_merge_inv3,
    'db_prefix': 'Insti3_',
    'first_date': '20041217'
}


tse_mb_config = {
    'file_name': 'StockInfo',
    'register_file': 'MB_Register',
    'check_date_file': 'CheckDay_MB',
    'grepper_function': get_tse_margin_balance,
    'db_prefix': 'MB_',
    'first_date': '20030102',
    #  '20010102'
}


otc_price_config = {
    'file_name': 'StockInfo_otc',
    'register_file': 'DB_Register',
    'check_date_file': 'CheckDay',
    'grepper_function': get_otc_one_day,
    'db_prefix': 'DB_',
    'first_date': '20070423'
}


otc_insti3_config = {
    'file_name': 'StockInfo_otc',
    'register_file': 'Insti3_Register',
    'check_date_file': 'CheckDay_insti3',
    'grepper_function': get_otc_insti3,
    'db_prefix': 'Insti3_',
    'first_date': '20041020'
}


otc_mb_config = {
    'file_name': 'StockInfo_otc',
    'register_file': 'MB_Register',
    'check_date_file': 'CheckDay_MB',
    'grepper_function': get_otc_margin_balance,
    'db_prefix': 'MB_',
    'first_date': '20070102',

}


def get_day_list(first_date, reference_file='IndexData'):
    #  Now Day
    now_time = datetime.now()
    day_list = []

    for year in range(first_date.year, now_time.year+1):
        file_name = '%s/%d.csv' % (reference_file, year)
        if os.path.isfile(file_name):
            df = pd.read_csv(file_name, dtype={'date': str})
            df['date'] = df['date'].apply(str2datetime)
            df = df[df['date'] >= first_date]
            day_list += df['date'].tolist()
    return day_list


class Collector:
    def __init__(self, config):
        # check file
        self.config = config
        if not os.path.isdir(config['file_name']):
            os.mkdir(config['file_name'])
        self.prefix = config['file_name']

        # Build DB_Register
        self.DB_Register_Addr = self.prefix + '/' + config['register_file'] + '.csv'
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
            self.engines[DB_idx] = create_engine('sqlite:///%s/%s%d.db' %
                                                 (self.prefix, config['db_prefix'], DB_idx),
                                                 echo=False)

    def save_register(self):
        self.DB_Register.reset_index(inplace=True)
        self.DB_Register.to_csv(self.DB_Register_Addr, index=False)
        self.DB_Register.set_index('StockCode', inplace=True)

    def save_df_by_name(self, table_name, df, db_idx):
        df.to_sql(name=table_name, con=self.engines[db_idx], if_exists='append', index=False)

    def save_df_to_db(self, table_name, df):
        # Check Table exist or not
        if table_name not in self.DB_Register.index:

            # choose the latest DB
            max_db_idx = self.DB_Register.DB_Idx.max()
            if np.isnan(max_db_idx):
                max_db_idx = 0
                self.engines[max_db_idx] = \
                    create_engine('sqlite:///%s/%s%d.db' %
                                  (self.prefix, self.config['db_prefix'], max_db_idx),
                                  echo=False)

            # if the latest DB's Table number is 500
            if len(self.engines[max_db_idx].table_names()) >= 500:
                # make a new DB
                max_db_idx += 1
                self.engines[max_db_idx] = \
                    create_engine('sqlite:///%s/%s%d.db' %
                                  (self.prefix, self.config['db_prefix'], max_db_idx),
                                  echo=False)

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
        date_diff = timedelta(days=1)

        #  Start Day
        check_day_path = self.prefix + '/' + self.config['check_date_file'] + '.txt'

        if os.path.isfile(check_day_path):
            with open(check_day_path, 'r', encoding='UTF-8') as file:
                check_day_str = file.read()
        else:
            check_day_str = self.config['first_date']

        start_year = int(check_day_str[0:4])
        start_month = int(check_day_str[4:6])
        start_day = int(check_day_str[6:])
        start_date = datetime(start_year, start_month, start_day)

        if os.path.isfile(check_day_path):
            start_date += date_diff

        day_list = get_day_list(first_date=start_date, reference_file='IndexData')

        # print(start_date)
        # print(day_list)
        grep_func = self.config['grepper_function']

        #  Start Loop
        last_success_day = 0
        save_list = []

        # cnt = 0
        for start_date in day_list:
            # cnt += 1
            # if cnt >= 25:
            #     break

            #  Print Log
            date_str = '{0}{1:02d}{2:02d}'.\
                format(start_date.year, start_date.month, start_date.day)
            # print(grep_func.__name__ + ' Read ' + date_str + )
            print("{0} Read {1} ; store que: {2}".
            	format(grep_func.__name__, date_str, len(save_list)))
            time.sleep(np.random.random() + 5)
            data_df = grep_func(start_date)

            if isinstance(data_df, DataFrame):
                #  success
                save_list.append(data_df)
                if len(save_list) >= 120:
                    print('Save')
                    self.save_all_data(save_list, date_str, check_day_path)
                    save_list = []
                last_success_day = date_str
            else:
                print('GET ' + date_str + ' Fail')
                raise MyError(-1)

            # start_date += date_diff

        if save_list:
            print('Last Save')
            self.save_all_data(save_list, last_success_day, check_day_path)


def collect_main(targets):
    for target in targets:

        if target == 'tse_price':
            input_config = tse_price_config
        elif target == 'tse_insti3':
            input_config = tse_insti3_config
        elif target == 'tse_mb':
            input_config = tse_mb_config
        elif target == 'otc_price':
            input_config = otc_price_config
        elif target == 'otc_insti3':
            input_config = otc_insti3_config
        elif target == 'otc_mb':
            input_config = otc_mb_config
        else:
            print(target + " not a config")
            break
        member = Collector(input_config)
        member.check_all_data()

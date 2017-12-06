__author__ = 'phlai'

import pandas as pd
# import numpy as np
from pandas import DataFrame

# from pandas_datareader import data
# from datetime import datetime, timedelta

from sqlalchemy import create_engine

# import requests
import urllib

# import re
import os
# import requests
# import time
#
# import logging
from .util import MyError


def get_actions(stock_code):
    if not isinstance(stock_code, str):
        print('stock code must be string')
        return -1

    action_resource = 'http://stock.wearn.com/dividend.asp?kind='
    max_col_time = 5
    for coll_time in range(max_col_time):
        try:
            html_input = pd.read_html(action_resource + stock_code)
            if html_input[0].shape[0] != 4:
                actions = html_input[0].copy()
                actions.columns = [actions.loc[1, 0], actions.loc[3, 0], actions.loc[3, 1],
                                   actions.loc[2, 1], actions.loc[3, 2], actions.loc[2, 3],
                                   actions.loc[2, 4], actions.loc[2, 5], actions.loc[1, 3]]
                actions.drop([0, 1, 2, 3], axis=0, inplace=True)
                actions.reset_index(inplace=True)
                actions.drop('index', axis=1, inplace=True)
                actions.fillna(-1, inplace=True)
                return actions
        except urllib.error.HTTPError:
            print('HTTPError')
        except urllib.error.URLError:
            print('URLError')
        except:
            print('Other Error')
    return -1


def get_all_data(stock_file):

    db_register_loc = stock_file + '/DB_Register.csv'
    db_register = pd.read_csv(db_register_loc,
                              dtype={'StockCode': str, 'DB_Idx': int})
    db_register.set_index('StockCode', inplace=True)

    #  build
    engines = {}
    for db_idx in db_register['DB_Idx'].unique():
        engines[db_idx] = create_engine('sqlite:///%s/Act_%d.db' % (stock_file, db_idx),
                                        echo=False)

    for row in db_register.itertuples():
        stock_code = str(row[0])
        db_idx = row[1]
        # print(stock_code)
        # print(type(stock_code))
        actions = get_actions(stock_code)
        if isinstance(actions, DataFrame):
            print(stock_code + '  get Action')
            #  success
            if engines[db_idx].has_table(stock_code):
                ori_table = pd.read_sql(stock_code, con=engines[db_idx])
                if not ori_table.equals(actions):
                    actions.to_sql(name=stock_code, con=engines[db_idx], if_exists='replace', index=False)
            else:
                actions.to_sql(name=stock_code, con=engines[db_idx], if_exists='replace', index=False)
        else:
            print(stock_code + '  NO Action')


class UpdateActions:

    def __init__(self, stock_files):
        if not isinstance(stock_files, list):
            print('stock files should be list')
            raise MyError(-1)

        for file in stock_files:
            if not os.path.isdir(file):
                print('file %s not exist' % file)
                raise MyError(-1)

        self.stock_files = stock_files

    def check_all_data(self):

        for file in self.stock_files:
            print('get ' + file)
            get_all_data(file)
        print('check_all_data  done')

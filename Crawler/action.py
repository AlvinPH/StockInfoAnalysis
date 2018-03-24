__author__ = 'phlai'

import pandas as pd
# import numpy as np
from pandas import DataFrame

# from pandas_datareader import data
from datetime import datetime

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
    max_col_time = 3
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
            else:
                return -1
        except urllib.error.HTTPError:
            print('HTTPError')
        except urllib.error.URLError:
            print('URLError')
        except KeyboardInterrupt:
            print('KeyboardInterrupt')
            return -1000
        except:
            print('Other Error')
    return -1


def get_all_data(stock_file):

    act_register_loc = stock_file + '/ActRegister.csv'
    check_old_data_flag = True
    if os.path.isfile(act_register_loc):
        act_register = pd.read_csv(act_register_loc,
                                   dtype={'StockCode': str, 'DB_Idx': int})
        engines = {}
        for db_idx in act_register['DB_Idx'].unique():
            engines[db_idx] = create_engine('sqlite:///%s/Act_%d.db' % (stock_file, db_idx), echo=False)
        store_idx = act_register['DB_Idx'].max()
    else:
        # init
        act_register = DataFrame(columns=['StockCode', 'DB_Idx'])
        store_idx = 0
        engines = {store_idx: create_engine('sqlite:///%s/Act_0.db' % stock_file, echo=False)}
        check_old_data_flag = False

    db_register_loc = stock_file + '/DB_Register.csv'
    db_register = pd.read_csv(db_register_loc,
                              dtype={'StockCode': str, 'DB_Idx': int})
    db_register.set_index('StockCode', inplace=True)
    db_engines = {}
    for db_idx in db_register['DB_Idx'].unique():
        db_engines[db_idx] = create_engine('sqlite:///%s/DB_%d.db' % (stock_file, db_idx), echo=False)

    current_year = datetime.today().year

    for row in db_register.itertuples():
        #  get Data from Internet
        stock_code = str(row[0])

        if check_old_data_flag:
            db_idx = row[1]
            price_df = pd.read_sql(stock_code, con=db_engines[db_idx])
            if price_df['date'].iloc[-1].year <= current_year-2:
                print(stock_code + '  is too old and collected before, PASS')
                continue

        actions = get_actions(stock_code)

        if isinstance(actions, DataFrame):
            #  success
            if stock_code in act_register.StockCode.values:
                #  if we have old data in Database
                engine_idx = act_register[act_register['StockCode'] == stock_code]['DB_Idx'][0]
                original_df = pd.read_sql(stock_code, con=engines[engine_idx])
                if original_df.equals(actions):
                    print(stock_code + " 's  Action NO Update")
                    continue
                else:
                    print(stock_code + " 's  Action Update")
                    actions.to_sql(name=stock_code, con=engines[engine_idx], if_exists='replace', index=False)
            else:
                print(stock_code + " got  new Action")
                #  New Data
                #  check store index
                if len(engines[store_idx].table_names()) >= 500:
                    store_idx += 1
                    engines[store_idx] = create_engine('sqlite:///%s/Act_%d.db' % (stock_file, store_idx), echo=False)
                    act_register.to_csv(act_register_loc, index=False)
                actions.to_sql(name=stock_code, con=engines[store_idx], if_exists='replace', index=False)
                register_insert_idx = act_register.shape[0]
                act_register.loc[register_insert_idx, 'StockCode'] = stock_code
                act_register.loc[register_insert_idx, 'DB_Idx'] = store_idx
        elif actions == -1000:
            break
        else:
            print(stock_code + '  NO Action')

    act_register.to_csv(act_register_loc, index=False)


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

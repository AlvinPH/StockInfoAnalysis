__author__ = 'phlai'
import pandas as pd
import numpy as np
from pandas import DataFrame

# from pandas_datareader import data
from datetime import datetime, timedelta

# from sqlalchemy import create_engine

import requests
# import urllib

import re
import os
import logging
import time


def get_month_indices_pay_report(spec_date):
    #  http://www.twse.com.tw/zh/page/trading/indices/MFI94U.html
    #  spec_date = datetime(2017, 11, 1)
    date_str = '{0}{1:02d}{2:02d}'.format(spec_date.year, spec_date.month, 1)
    url = 'http://www.twse.com.tw/indicesReport/MFI94U'
    query_params = {
        'response': 'json',
        'date': date_str,
        '_': str(round(time.time() * 1000) - 500)
    }
    # Get json data
    page = requests.get(url, params=query_params)
    if not page.ok:
        logging.error("Can not get data at {}".format(date_str))
    content = page.json()

    if 'data' not in content:
        return -1
    df = DataFrame(content['data'])

    def proc(x):
        t = x.split('/')
        return datetime(int(t[0]) + 1911, int(t[1]), int(t[2]))

    df[0] = df[0].apply(proc)
    df[1] = df[1].apply(lambda x: float(re.sub("[, ]", "", x)))  # clear comma
    df.columns = ['date', 'price']

    return df


def get_month_indices_report(spec_date):
    #  http://www.twse.com.tw/indicesReport/MI_5MINS_HIST?response=json&date=20171124&_=1511532428178
    #  spec_date = datetime(2017, 11, 1)
    date_str = '{0}{1:02d}{2:02d}'.format(spec_date.year, spec_date.month, spec_date.day)
    url = 'http://www.twse.com.tw/indicesReport/MI_5MINS_HIST'
    query_params = {
        'response': 'json',
        'date': date_str,
        '_': str(round(time.time() * 1000) - 500)
    }
    # Get json data
    page = requests.get(url, params=query_params)
    if not page.ok:
        logging.error("Can not get data at {}".format(date_str))
    content = page.json()
#     return content
    if 'data' not in content:
        return -1
    df = DataFrame(content['data'])

    def proc(x):
        t = x.split('/')
        return datetime(int(t[0]) + 1911, int(t[1]), int(t[2]))

    df[0] = df[0].apply(proc)
    df[1] = df[1].apply(lambda x: float(re.sub("[, ]", "", x)))  # clear comma
    df.columns = ['date', 'open', 'high', 'low', 'close']

    return df


def add_a_month(input_day):
    one_month = timedelta(days=45)
    input_day = datetime(input_day.year, input_day.month, 1)
    output_day = input_day + one_month
    output_day = datetime(output_day.year, output_day.month, 1)
    return output_day


def get_all_indices_pay_data(save_dir):
    #  http://www.twse.com.tw/zh/page/trading/indices/MFI94U.html
    #  from 2003/1/1 ~

    if not os.path.isdir(save_dir):
        os.mkdir(save_dir)

    #  YYYY.csv
    start_year = 2003
    for file in os.listdir(save_dir):
        if file.startswith('.'):
            continue
        file_year = int(file[:4])
        if file_year > start_year:
            start_year = file_year

    last_file_name = save_dir + '/' + ('%d.csv' % start_year)
    if os.path.isfile(last_file_name):
        df = pd.read_csv(last_file_name, dtype={'date': str, 'price': float})

        def proc(x):
            t = x.split('-')
            return datetime(int(t[0]), int(t[1]), int(t[2]))

        df['date'] = df['date'].apply(proc)
        start_date = df['date'].max() + timedelta(days=1)
    else:
        df = DataFrame(columns=['date', 'price'])
        start_date = datetime(2003, 1, 1)

    today = datetime.now()

    while start_date <= today:
        print('Index Pay {0}'.format(start_date))
        data = get_month_indices_pay_report(start_date)
        if isinstance(data, DataFrame):
            data = data[data['date'] >= start_date]
            if data.empty:
                break
            else:
                df = pd.concat([df, data])
        next_month = add_a_month(start_date)
        if (next_month.year != start_date.year) or (next_month > today):
            df.to_csv(save_dir + '/%d.csv' % start_date.year, index=False)
            df = DataFrame(columns=['date', 'price'])

        start_date = next_month
        time.sleep(np.random.random() + 3)


def get_all_indices_data(save_dir):
    #  http://www.twse.com.tw/zh/page/trading/indices/MFI94U.html
    #  from 1999/01/05 ~

    if not os.path.isdir(save_dir):
        os.mkdir(save_dir)

    #  YYYY.csv
    start_year = 1999
    for file in os.listdir(save_dir):
        if file.startswith('.'):
            continue
        file_year = int(file[:4])
        if file_year > start_year:
            start_year = file_year

    last_file_name = save_dir + '/' + ('%d.csv' % start_year)
    if os.path.isfile(last_file_name):
        df = pd.read_csv(last_file_name, dtype={'date': str, 'open': float,
                                                'high': float, 'low': float,
                                                'close': float})

        def proc(x):
            t = x.split('-')
            return datetime(int(t[0]), int(t[1]), int(t[2]))

        df['date'] = df['date'].apply(proc)
        start_date = df['date'].max() + timedelta(days=1)
    else:
        df = DataFrame(columns=['date', 'open', 'high', 'low', 'close'])
        start_date = datetime(2003, 1, 1)

    today = datetime.now()

    while start_date <= today:
        print('Index {0}'.format(start_date))
        data = get_month_indices_report(start_date)
        if isinstance(data, DataFrame):
            data = data[data['date'] >= start_date]
            if data.empty:
                break
            else:
                df = pd.concat([df, data])
        next_month = add_a_month(start_date)
        if (next_month.year != start_date.year) or (next_month > today):
            df.to_csv(save_dir + '/%d.csv' % start_date.year, index=False)
            df = DataFrame(columns=['date', 'open', 'high', 'low', 'close'])

        start_date = next_month
        time.sleep(np.random.random() + 3)
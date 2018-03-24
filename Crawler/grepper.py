__author__ = 'phlai'


import pandas as pd
import numpy as np
from pandas import DataFrame

# from pandas_datareader import data
from datetime import datetime

# from sqlalchemy import create_engine

# import requests
import urllib

import re
# import os
import requests
import time

import logging

from .util import clear_comma

#  三大法人(機構投資人) Institutional investors
#  買賣超 Net Buy / Net Ssell
#  散戶 Retail Investors
#  大盤指數 (TSE) index/ TAIEX
#  外資 Foreign Investor(s) / Foreign Investment Institution
#  投信 Investment Trust / Domestic Institution (用於與外資作區別時)
#  自營商 Dealer

# _filter_len = 4
_try_time = 3


def get_tse_one_day(spec_date):
    #  source: http://www.twse.com.tw/zh/page/trading/exchange/MI_INDEX.html
    date_str = '{0}{1:02d}{2:02d}'.format(spec_date.year, spec_date.month, spec_date.day)
    url = 'http://www.twse.com.tw/exchangeReport/MI_INDEX'
    #  ALL => all data
    #  ALLBUT0999 => all data, no
    # Get json data
    for loop1 in range(_try_time):
        query_params = {
            'date': date_str,
            'response': 'json',
            # 'type': 'ALLBUT0999',
            'type': 'ALL',
            '_': str(round(time.time() * 1000) - 500)
        }
        page = requests.get(url, params=query_params)
        if not page.ok:
            logging.error("Can not get TSE data at {}".format(date_str))
        content = page.json()
        isoffday = True
        key = -1
        for key in content.keys():
            if isinstance(content[key], list):
                if len(content[key][0]) == 16:
                    isoffday = False
                    break
        if isoffday:
            time.sleep(3)
            continue

        data_df = DataFrame(data=content[key],
                            columns=['code', 'name', 'volume', 'transaction', 'turnover',
                                     'open', 'high', 'low', 'close', 'UD', 'difference',
                                     'last_buy', 'last_buy_volume',
                                     'last_sell', 'last_sell_volume', 'PE_ratio'])
        #  clear comma
        data_df = data_df.applymap(lambda x: re.sub(",", "", x))
        data_df.replace({'UD': {'<p style= color:red>+</p>': '+',
                                '<p style= color:green>-</p>': '-'}},
                        inplace=True)
        data_df['date'] = spec_date

        return data_df

    return -1


def get_otc_one_day(spec_date):
    #  source: http://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote.php?l=zh-tw
    date_str = '{0}/{1:02d}/{2:02d}'.format(spec_date.year-1911, spec_date.month, spec_date.day)
    for loop1 in range(_try_time):
        ttime = str(int(time.time()*100))
        url = 'http://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/' \
            'stk_quote_result.php?l=zh-tw&d={}&_={}'.format(date_str, ttime)
        page = requests.get(url)
        if not page.ok:
            logging.error("Can not get OTC data at {}".format(date_str))
        content = page.json()

        if (len(content['aaData']) + len(content['mmData'])) == 0:
            time.sleep(3)
            continue
        data_df = DataFrame(data=content['aaData'] + content['mmData'],
                            columns=['code', 'name', 'close', 'difference', 'open',
                                     'high', 'low', 'avg', 'volume', 'turnover',
                                     'transaction', 'last_buy',
                                     'last_sell', 'NumOfShare', 'NextRefPrice',
                                     'NextUpperPrice', 'NextLowerPrice'])
        # data_df = data_df.applymap(lambda x: re.sub(",", "", x))  # clear comma
        data_df = data_df.applymap(clear_comma)
        data_df['date'] = spec_date
        return data_df
    return -1


#  GET TSE Insti3 Data
def get_insti3(spec_date):
    date_str = '{0}{1:02d}{2:02d}'.format(spec_date.year, spec_date.month, spec_date.day)
    url = 'http://www.twse.com.tw/fund/T86'
    for loop1 in range(_try_time):
        query_params = {
            'date': date_str,
            'response': 'json',
            'selectType': 'ALLBUT0999',
            '_': str(round(time.time() * 1000) - 500)
        }
        # Get json data
        page = requests.get(url, params=query_params)
        if not page.ok:
            logging.error("Can not get TSE data at {}".format(date_str))
        content = page.json()
        if 'data' not in content:
            time.sleep(3)
            continue
        col = [f.replace('</br>', ' ') for f in content['fields']]
        df = DataFrame(content['data'], columns=col)
        # df = df.applymap(lambda x: re.sub("[, ]", "", x))  # clear comma
        df = df.applymap(clear_comma)
        return df
    return -1


def get_foreinv(spec_date):
    #  TSE Foreign Investment
    #  source: http://www.twse.com.tw/zh/page/trading/fund/TWT38U.html
    date_str = '{0}{1:02d}{2:02d}'.format(spec_date.year, spec_date.month, spec_date.day)
    url = 'http://www.twse.com.tw/fund/TWT38U'
    for loop1 in range(_try_time):
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
            time.sleep(3)
            continue
        # col = content['fields']
        # col.remove('')
        # col = ["F_"+col[x] if x > 0 else col[x] for x in range(len(col))]
        col = ["證券代號", "F_證券名稱", "F_買進股數", "F_賣出股數", "F_買賣超股數"]
        df = DataFrame(content['data'])
        df.drop(0, axis=1, inplace=True)
        if df.shape[1] == 11:
            df = df[[1,2, 9, 10, 11]]
        df.columns = col
        # df = df.applymap(lambda x: re.sub("[, ]", "", x))  # clear comma
        df = df.applymap(clear_comma)
        df.set_index("證券代號", inplace=True)
        # df = df[df.index.str.len() <= _filter_len]
        return df
    return -1


def get_domeins(spec_date):
    #  source: http://www.twse.com.tw/zh/page/trading/fund/TWT44U.html
    date_str = '{0}{1:02d}{2:02d}'.format(spec_date.year, spec_date.month, spec_date.day)
    url = 'http://www.twse.com.tw/fund/TWT44U'

    for loop1 in range(_try_time):
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
            time.sleep(3)
            continue
        col = content['fields']
        col.remove('')
        col = ["I_"+col[x] if x > 0 else col[x] for x in range(len(col))]
        df = DataFrame(content['data'])
        df.drop(0, axis=1, inplace=True)
        df.columns = col
        # df = df.applymap(lambda x: re.sub("[, ]", "", x))  # clear comma
        df = df.applymap(clear_comma)
        df.set_index("證券代號", inplace=True)
        # df = df[df.index.str.len() <= _filter_len]
        return df
    return -1


def get_dealer(spec_date):
    #  source: http://www.twse.com.tw/zh/page/trading/fund/TWT43U.html
    date_str = '{0}{1:02d}{2:02d}'.format(spec_date.year, spec_date.month, spec_date.day)
    url = 'http://www.twse.com.tw/fund/TWT43U'
    #  http://www.twse.com.tw/fund/TWT43U?response=json&date=20171006&_=1507529758676
    for loop1 in range(_try_time):
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
            time.sleep(3)
            continue
        if len(content['fields']) == 11:
            col = ['證券代號', 'D_證券名稱', 'D_買進股數(自行)', 'D_賣出股數(自行)', 'D_買賣超股數(自行)',
                   'D_買進股數(避險)', 'D_賣出股數(避險)', 'D_買賣超股數(避險)',
                   'D_買進股數', 'D_賣出股數', 'D_買賣超股數']
        else:
            col = content['fields']
            col = ["D_"+col[x] if x > 0 else col[x] for x in range(len(col))]
        df = DataFrame(content['data'], columns=col)
        if len(content['fields']) != 11:
            df['D_買進股數(自行)'] = df['D_買進股數']
            df['D_賣出股數(自行)'] = df['D_賣出股數']
            df['D_買賣超股數(自行)'] = df['D_買賣超股數']
            df['D_買進股數(避險)'] = '0'
            df['D_賣出股數(避險)'] = '0'
            df['D_買賣超股數(避險)'] = '0'
            df = df[['證券代號', 'D_證券名稱', 'D_買進股數(自行)', 'D_賣出股數(自行)', 'D_買賣超股數(自行)',
                     'D_買進股數(避險)', 'D_賣出股數(避險)', 'D_買賣超股數(避險)',
                     'D_買進股數', 'D_賣出股數', 'D_買賣超股數']]
        # df = df.applymap(lambda x: re.sub("[, ]", "", x))  # clear comma
        df = df.applymap(clear_comma)
        df.set_index("證券代號", inplace=True)
        # df = df[df.index.str.len() <= _filter_len]
        return df
    return -1


def get_merge_inv3(spec_date):

    fore = get_foreinv(spec_date)
    time.sleep(np.random.random() + 3)
    dome = get_domeins(spec_date)
    time.sleep(np.random.random() + 3)
    deal = get_dealer(spec_date)

    con_df = []

    if isinstance(fore, DataFrame):
        con_df.append(fore)
    if isinstance(dome, DataFrame):
        con_df.append(dome)
    if isinstance(deal, DataFrame):
        con_df.append(deal)
    # if len(con_df) != 3:
    #     return -1
    # else:

    mrg = pd.concat(con_df, axis=1)
    mrg['date'] = spec_date
    mrg.fillna('0', inplace=True)
    mrg.index.name = 'code'
    mrg.reset_index(inplace=True)
    # mrg.rename(columns={'證券代號': 'code'}, inplace=True)
    return mrg


#  GET OTC Insti3 Data
def get_otc_insti3_p1(spec_date):
    #  http://hist.tpex.org.tw/Hist/STOCK/3INSTI/3INSTITRA.HTML
    #  http://hist.tpex.org.tw/Hist/STOCK/3INSTI/DAILY_TRADE/BIGD930601S_N.html
    #  2004/06/01 ~ 2006/12/29
    # lowerbound = datetime(2004, 6, 1)
    lowerbound = datetime(2004, 10, 20)
    upperbound = datetime(2006, 12, 29)

    if spec_date < lowerbound or spec_date > upperbound:
        return -1
    day_str = '{0}{1:02d}{2:02d}'.format(spec_date.year - 1911, spec_date.month, spec_date.day)
    url = 'http://hist.tpex.org.tw/Hist/STOCK/3INSTI/DAILY_TRADE/BIGD%sS_N.html' % day_str
    for coll_time in range(_try_time):
        try:
            html_input = pd.read_html(url)
            result = html_input[0]
            # result = result.applymap(lambda x: re.sub("[, ]", "", x))  # clear comma
            result = result.applymap(clear_comma)
            return result
        except urllib.error.HTTPError:
            print('HTTPError')
        except urllib.error.URLError:
            print('URLError')
        except:
            print('Other Error')
    return -1


def get_otc_insti3_p2(spec_date):
    #  http://www.tpex.org.tw/storage/zh-tw/web/stock/3insti/daily_trade/3itradeHis.htm
    #  http://hist.gretai.org.tw/hist/stock/3insti/DAILY_TRADE/BIGD_960420S_Q.html
    #  2007/01/02 ~ 2007/04/20
    lowerbound = datetime(2007, 1, 2)
    upperbound = datetime(2007, 4, 20)

    if spec_date < lowerbound or spec_date > upperbound:
        return -1

    day_str = '{0}{1:02d}{2:02d}'.format(spec_date.year - 1911, spec_date.month, spec_date.day)
    url = 'http://hist.gretai.org.tw/hist/stock/3insti/DAILY_TRADE/BIGD_%sS_Q.html' % day_str
    for coll_time in range(_try_time):
        try:
            html_input = pd.read_html(url, encoding='big5')
            result = html_input[1]
            # result = result.applymap(lambda x: re.sub("[, ]", "", x) if isinstance(x, str))  # clear comma
            result = result.applymap(clear_comma)
            return result
        except urllib.error.HTTPError:
            print('HTTPError')
        except urllib.error.URLError:
            print('URLError')
        except:
            print('Other Error')
    return -1


def get_otc_insti3_p3(spec_date):
    #  http://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade.php?l=zh-tw
    #  http://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_result.php?
    #  l=zh-tw&se=EW&t=D&d=103/11/19&_=1508346850898
    #
    #  http://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge.php?l=zh-tw
    #  http://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?
    #  l=zh-tw&se=EW&t=D&d=106/10/05&_=1508345330544
    #  2007/04/20 ~ 2014/11/28 , 2014/12/01 ~ present
    #  
    #  http://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&se=AL&t=D&d=106/12/13&_=1513275131548

    lowerbound = datetime(2007, 4, 20)
    if spec_date < lowerbound:
        return -1

    date_str = '{0}/{1:02d}/{2:02d}'.format(spec_date.year - 1911, spec_date.month, spec_date.day)
    url = 'http://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_result.php'
    if spec_date >= datetime(2014, 12, 1):
        url = 'http://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php'
    for loop1 in range(_try_time):
        query_params = {
            'l': 'zh-tw',
            # 'se': 'EW',
            'se': 'AL',
            't': 'D',
            'd': date_str,
            '_': str(round(time.time() * 1000) - 500)
        }
        # Get json data
        page = requests.get(url, params=query_params)
        if not page.ok:
            logging.error("Can not get TSE data at {}".format(date_str))
        content = page.json()
        if not content['aaData']:
            time.sleep(3)
            continue
        df = DataFrame(content['aaData'])
        # df = df.applymap(lambda x: re.sub("[, ]", "", x))  # clear comma
        df = df.applymap(clear_comma)
        return df
    return -1


def get_otc_insti3(spec_date):

    if spec_date < datetime(2014, 12, 1):
        if spec_date <= datetime(2007, 1, 1):
            df = get_otc_insti3_p1(spec_date)
            if isinstance(df, int):
                return -1
            df.drop(0, axis=0, inplace=True)
        elif spec_date <= datetime(2007, 4, 20):
            df = get_otc_insti3_p2(spec_date)
        else:
            df = get_otc_insti3_p3(spec_date)

        if isinstance(df, int):
                return -1

        col_label = ['code', '證券名稱', '外資買', '外資賣', '外資買賣',
                     '投信買', '投信賣', '投信買賣',
                     '自營買', '自營賣', '自營買賣',
                     '三大買賣']
        df.columns = col_label
        
        df['自營買(自行)'] = df['自營買']
        df['自營賣(自行)'] = df['自營賣']
        df['自營買賣(自行)'] = df['自營買賣']
        df['自營買(避險)'] = df['自營賣(避險)'] = df['自營買賣(避險)'] = 0.0
        col_tem = ['外資買', '外資賣', '外資買賣',
                   '投信買', '投信賣', '投信買賣',
                   '自營買(自行)', '自營賣(自行)', '自營買賣(自行)',
                   '自營買(避險)', '自營賣(避險)', '自營買賣(避險)']
        df[col_tem] = df[col_tem].astype(float)
    else:
        df = get_otc_insti3_p3(spec_date)
        if isinstance(df, int):
                return -1
        if df.shape[1] != 25:# fix at 20180322
            if df.shape[1] == 17:
                df.drop(16, axis=1, inplace=True)
            
            col_label = ['code', '證券名稱', '外資買', '外資賣', '外資買賣',
                         '投信買', '投信賣', '投信買賣',
                         '自營買賣', '自營買(自行)', '自營賣(自行)', '自營買賣(自行)',
                         '自營買(避險)', '自營賣(避險)', '自營買賣(避險)', '三大買賣']
            df.columns = col_label
            col_tem = ['外資買', '外資賣', '外資買賣',
                       '投信買', '投信賣', '投信買賣',
                       '自營買(自行)', '自營賣(自行)', '自營買賣(自行)',
                       '自營買(避險)', '自營賣(避險)', '自營買賣(避險)']
            df[col_tem] = df[col_tem].astype(float)
            df['自營買'] = df['自營買(自行)'] + df['自營買(避險)']
            df['自營賣'] = df['自營賣(自行)'] + df['自營賣(避險)']
            df['自營買賣'] = df['自營買賣(自行)'] + df['自營買賣(避險)']
            
        elif df.shape[1] == 25:
            
            df.drop([2,3,4,5,6,7,24], axis=1, inplace=True)
            
            col_label = ['code', '證券名稱', '外資買', '外資賣', '外資買賣',
                         '投信買', '投信賣', '投信買賣',
                         '自營買(自行)', '自營賣(自行)', '自營買賣(自行)',
                         '自營買(避險)', '自營賣(避險)', '自營買賣(避險)',
                         '自營買', '自營賣', '自營買賣', '三大買賣']
            df.columns = col_label
            col_tem = ['外資買', '外資賣', '外資買賣',
                       '投信買', '投信賣', '投信買賣',
                       '自營買(自行)', '自營賣(自行)', '自營買賣(自行)',
                       '自營買(避險)', '自營賣(避險)', '自營買賣(避險)',
                       '自營買', '自營賣', '自營買賣']
            df[col_tem] = df[col_tem].astype(float)
            

    col_label_final = ['code', '證券名稱', '外資買', '外資賣', '外資買賣',
                       '投信買', '投信賣', '投信買賣',
                       '自營買(自行)', '自營賣(自行)', '自營買賣(自行)',
                       '自營買(避險)', '自營賣(避險)', '自營買賣(避險)',
                       '自營買', '自營賣', '自營買賣', '三大買賣']
    df = df[col_label_final]
    if df['code'].dtype != 'O':
        df['code'] = df['code'].astype(str)
    # df = df[df['code'].str.len() <= _filter_len]
    # df = df.applymap(lambda x: re.sub("[, ]", "", x))  # clear comma
    df['date'] = spec_date

    return df


def get_tse_margin_balance(spec_date):
    #  TSE Margin Balance
    #  http://www.twse.com.tw/zh/page/trading/exchange/MI_MARGN.html
    #  http://www.twse.com.tw/exchangeReport/MI_MARGN?response=json&date=20171025&selectType=ALL&_=1508944964278
    #  2001/1/2 ~ present
    date_str = '{0}{1:02d}{2:02d}'.format(spec_date.year, spec_date.month, spec_date.day)
    url = 'http://www.twse.com.tw/exchangeReport/MI_MARGN'
    for loop1 in range(_try_time):
        query_params = {
            'response': 'json',
            'date': date_str,
            'selectType': 'ALL',
            '_': str(round(time.time() * 1000) - 500)
        }
        # Get json data
        page = requests.get(url, params=query_params)
        if not page.ok:
            logging.error("Can not get data at {}".format(date_str))
        content = page.json()
        if 'data' not in content:
            time.sleep(3)
            continue
        if len(content['data']) == 0:
            time.sleep(3)
            continue
        col_label = ['code', '股票名稱',
                     '融資買進', '融資賣出', '融資現金償還', '融資前日餘額', '融資今日餘額', '融資限額',
                     '融券買進', '融券賣出', '融券現金償還', '融券前日餘額', '融券今日餘額', '融券限額',
                     '資券互抵', '註記']
        df = DataFrame(content['data'], columns=col_label)
        df = df.applymap(lambda x: re.sub("[, ]", "", x))  # clear comma
        df.set_index("code", inplace=True)
        # df = df[df.index.str.len() <= _filter_len]
        df.reset_index(inplace=True)
        df['date'] = spec_date
        return df
    return -1


def get_otc_margin_balance(spec_date):
    #  OTC Margin Balance
    #  http://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal.php?l=zh-tw
    #  http://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal.php?l=zh-tw
    #  2007/1/2 ~ present
    date_str = '{0}/{1:02d}/{2:02d}'.format(spec_date.year-1911, spec_date.month, spec_date.day)
    url = 'http://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_result.php'
    for loop1 in range(_try_time):
        query_params = {
            'l': 'zh-tw',
            'd': date_str,
            '_': str(round(time.time() * 1000) - 500)
        }
        # Get json data
        page = requests.get(url, params=query_params)
        if not page.ok:
            logging.error("Can not get data at {}".format(date_str))
        content = page.json()
        if 'aaData' not in content:
            time.sleep(3)
            continue
        if not content['aaData']:
            time.sleep(3)
            continue
        col_label = ['code', 'name', '前資餘額(張)', '資買', '資賣', '現償', '資餘額',
                     '資屬證金', '資使用率(%)', '資限額', '前券餘額(張)', '券賣', '券買',
                     '券償', '券餘額', '券屬證金', '券使用率(%)', '券限額', '資券相抵(張)', '備註']
        df = DataFrame(content['aaData'], columns=col_label)
        df = df.applymap(lambda x: re.sub("[, ]", "", x))  # clear comma
        df.set_index("code", inplace=True)
        # df = df[df.index.str.len() <= _filter_len]
        df.reset_index(inplace=True)
        df['date'] = spec_date
        return df
    return -1

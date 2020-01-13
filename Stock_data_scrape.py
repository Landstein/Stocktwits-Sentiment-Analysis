import re
import os
import time
import random
import requests
import numpy as np
import pandas as pd
from os import system
from math import floor
from copy import deepcopy
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from alpha_vantage.timeseries import TimeSeries
import matplotlib.pyplot as plt


import time
import schedule
from selenium.webdriver.chrome.options import Options
import config
import matplotlib.pyplot as plt

from sqlalchemy import create_engine

from datetime import date, datetime
import holidays
today = date.today()
us_holidays = holidays.UnitedStates()

today in us_holidays


def stocktwits_login():
    pd.set_option('display.max_columns', 20)
    pd.set_option('display.max_colwidth', 200)
    mobile_emulation = {"deviceName": "iPhone X"}

    chrome_options = Options()
    chrome_options.add_experimental_option("mobileEmulation", mobile_emulation)
    driver = webdriver.Chrome(chrome_options=chrome_options)

    driver.get('https://stocktwits.com/')

    time.sleep(.5)
    login = driver.find_element_by_xpath(
        '//*[@id="app"]/div/div/div[2]/div/div/div/div/div[1]/div[1]/div[4]/div/span[2]')
    login.click()

    # username
    username = driver.find_element_by_xpath('//*[@id="app"]/div/div/div[4]/div[2]/div/form/div[1]/div[1]/label/input')
    username.click()
    username.send_keys(config.username)

    # password
    password = driver.find_element_by_xpath('//*[@id="app"]/div/div/div[4]/div[2]/div/form/div[1]/div[2]/label/input')
    password.click()
    password.send_keys(config.password)

    # sign in
    signin = driver.find_element_by_xpath('//*[@id="app"]/div/div/div[4]/div[2]/div/form/div[2]/div[1]/button')
    signin.click()

    return driver


def stock_data():
    tickers = ['ABBV', 'ATVI', 'ADBE', 'AMD', 'GOOGL', 'GOOG', 'MO', 'AMZN', 'AAL', 'AAPL', 'AMAT', 'T', 'BAC', 'BRK.B',
               'BBY', 'BIIB', 'BA', 'BKNG', 'BMY', 'AVGO', 'CAT', 'CVX', 'CMG', 'CSCO', 'C', 'KO', 'COST', 'CVS', 'DAL',
               'EBAY', 'EA', 'XOM', 'FB', 'FDX', 'F', 'FCX', 'GE', 'GM', 'GILD', 'GS', 'HD', 'INTC', 'IBM', 'ISRG', 'JNJ',
               'JPM', 'KMI', 'KHC', 'KR', 'LMT', 'M', 'MA', 'MCD', 'MRK', 'MU', 'MSFT', 'NFLX', 'NKE', 'NVDA', 'ORCL',
               'PYPL', 'PFE', 'PG', 'QCOM', 'CRM', 'SWKS', 'LUV', 'SBUX', 'TMUS', 'TTWO', 'TGT', 'TWTR', 'ULTA', 'UAA',
               'VZ', 'V', 'WMT', 'DIS', 'WFC', 'WYNN']

    stock_dic = {}
    stock_list = []

    driver = stocktwits_login()
    time.sleep(1)
    today = date.today()
    datecurrent_date = today.strftime("%Y-%m-%d")

    for i in tickers:
        key = ''
        ts = TimeSeries(key)
        driver.get(f'https://stocktwits.com/symbol/{i}')
        sentiment = driver.find_element_by_xpath(
            '//*[@id="app"]/div/div/div[2]/div[2]/div[2]/div[1]/div/main/section/div[1]/div/div/div/div/div[1]/div/div/div[2]/div/div[1]')
        sent = sentiment.text
        sent_color = driver.find_elements_by_class_name('st_rmCmCyZ')
        sent_color = sent_color[1]

        if sent_color.value_of_css_property('color') == 'rgba(255, 62, 62, 1)':
            stock_dic['Direction'] = 'Negetive'
        else:
            stock_dic['Direction'] = 'Positve'
        stock_dic['Stock'] = i
        stock_dic['Date'] = datecurrent_date
        stock_dic[f'{i}_Sentiment'] = sent
        stock, meta = ts.get_daily(symbol=i)
        stock_dic[f'{i}_Open_Price'] = stock[f'{datecurrent_date}']['1. open']
        stock_dic[f'{i}_Close_Price'] = stock[f'{datecurrent_date}']['4. close']
        stock_dic[f'{i}_Volume'] = stock[f'{datecurrent_date}']['5. volume']
        stock_list.append(stock_dic)
        stock_dic = {}
        if i != tickers[-1]:
            time.sleep(14)

    return stock_list


def stock_data_list(stock_list):
    datecurrent_date = today.strftime("%Y-%m-%d")
    new_list = []
    final_list = []
    for i in stock_list:
        hourly_date = datetime.now().strftime("%H:%M:%S")
        stock = i['Stock']
        sent = i[f'{stock}_Sentiment']
        sent = float(sent[:4])
        date = i['Date']
        open_p = i[f'{stock}_Open_Price']
        close_p = i[f'{stock}_Close_Price']
        volume = i[f'{stock}_Volume']

        if i['Direction'] == 'Negetive':
            sent = -abs(sent)
        else:
            sent = abs(sent)

        new_list.append(date)
        new_list.append(hourly_date)
        new_list.append(stock)
        new_list.append(sent)
        new_list.append(open_p)
        new_list.append(close_p)
        new_list.append(volume)
        final_list.append(new_list)
        new_list = []
    return final_list


def create_df(listy):
    df = pd.DataFrame(listy, columns=['date', 'h_m_s', 'stock', 'sentiment', 'open', 'close', 'volume'])
    df.set_index('date', inplace=True)
    df.index = pd.DatetimeIndex(df.index)
    return df


def sql_commit(df):
    engine = create_engine("mysql+mysqlconnector://{user}:{pw}@{host}/{db}"
                           .format(user=config.user,
                                   pw=config.passwd,
                                   host=config.host,
                                   db=config.db_name))

    stocks = df['stock'].values.tolist()
    for i in range(len(stocks)):
        stock = df[df['stock'] == stocks[i]]
        # Insert whole DataFrame into MySQL
        stock.to_sql(stocks[i], con=engine, if_exists='append')



def main():
    stock_list = stock_data()
    list_of_stocks = stock_data_list(stock_list)
    df = create_df(list_of_stocks)
    sql_commit(df)


schedule.every().day.at("9:30").do(main)
schedule.every().day.at("13:30").do(main)
schedule.every().day.at("15:55").do(main)


while True:
    schedule.run_pending()
    time.sleep(1)



from time import sleep
from os import environ
import json
import logging
import logging.config
import logging.handlers

import asyncio

from binance.client import Client
from binance.exceptions import BinanceAPIException
import ta

from pandas import DataFrame
import pandas as pd

import pyfiglet
from rich import print


API_KEY = environ.get('API_KEY')
SECRET = environ.get('SECRET')
LOGGER = logging.getLogger('bot')

HISTORY = []


def init_logging():
    logging.config.dictConfig(get_log_config())


def get_log_config() -> dict:
    with open('../LocalLogConfig.json', 'r') as config:
        return json.load(config)


def klines(symbol: str, interval: str, start_str: str) -> DataFrame:
    """Requests pair data from binance api"""
    try:
        symbol_data = pd.DataFrame(client.get_historical_klines(
            symbol=symbol,
            interval=interval,
            start_str=start_str
        ))
        symbol_data = symbol_data.iloc[:, :6]
        symbol_data.columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume']
        symbol_data = symbol_data.set_index('Time')
        symbol_data.index = pd.to_datetime(symbol_data.index, unit='ms')
        return symbol_data
    except BinanceAPIException as e:
        LOGGER.exception(e)
        sleep(60)
        return klines(symbol, interval, start_str)


def buy_signal(symbol_data: DataFrame) -> bool:
    """Checks whether the macd metric has moved from negative to positive values."""
    return ta.trend.macd_diff(symbol_data.Close).iloc[-1] > 0 > ta.trend.macd_diff(symbol_data.Close).iloc[-2]


def sell_signal(symbol_data: DataFrame) -> bool:
    """Checks whether the macd metric has moved from positive to negative values."""
    return ta.trend.macd_diff(symbol_data.Close).iloc[-1] < 0 < ta.trend.macd_diff(symbol_data.Close).iloc[-2]


async def print_1h_change():
    """Outputs the percentage of change in the price of ETH if
    the price has changed by more than 1% in the last hour."""
    candles = klines('ETHUSDT', '1h', '2h UTC')
    h_change = (float(candles.Close[1]) - float(candles.Open[1])) * 100 / float(candles.Open[1])

    if abs(h_change) >= 1:
        LOGGER.debug(f'{h_change:.02}%')

    await asyncio.sleep(5)
    await print_1h_change()


async def entry_point_search(symbol: str, quantity: int, signal=buy_signal):
    """Waits for a buy or sell signal, when it is received, recursively changes the signal search strategy."""
    global HISTORY
    buy_price = 0
    while True:
        symbol_data = klines(symbol, '1m', 'start_str')
        if signal(symbol_data):
            if signal.__name__ == 'buy_signal':
                # order = client.futures_create_order(symbol=symbol, side='BUY', type='MARKET', quantity=quantity)

                buy_price = float(symbol_data.Close[-1])

                LOGGER.info(f'Buy signal. New futures order. Price: {buy_price} Quantity: {quantity}')

                await entry_point_search(symbol, quantity, sell_signal)
            else:
                # order = client.futures_create_order(symbol=symbol, side='SELL', type='MARKET', quantity=quantity)

                sell_price = float(symbol_data.Close[-1])
                percentage_change = 100 - (100 * buy_price) / sell_price
                HISTORY.append((buy_price, sell_price, percentage_change))
                apr = sum(order[2] for order in HISTORY)
                positive = sum(1 if order[1] - order[0] >= 0 else 0 for order in HISTORY)

                LOGGER.info(f'Sell signal. New futures order. Price: {sell_price} Quantity: {quantity}')
                LOGGER.info(f'Sum all orders: {apr}%\nCount orders: {len(HISTORY)}\nPositive orders: {positive}')

                await entry_point_search(symbol, quantity, buy_signal)


if __name__ == '__main__':
    print(pyfiglet.figlet_format('K A M T O R', font='larry3d', width=400))
    init_logging()
    LOGGER.info('Bot was initialized.')
    client = Client(api_key=API_KEY, api_secret=SECRET)
    LOGGER.info('Client was initialized.')
    asyncio.run(print_1h_change())
    asyncio.run(entry_point_search('ETHBTC', 0))

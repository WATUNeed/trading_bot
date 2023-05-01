import asyncio
import datetime
from os import environ

from binance.client import Client
import pandas as pd
import ta
from pandas import DataFrame
from binance.exceptions import BinanceAPIException
from time import sleep


API_KEY = environ.get('API_KEY')
SECRET = environ.get('SECRET')

HISTORY = []


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
        print(e)
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
        print(f'{h_change:.02}%')

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

                print(f'{datetime.datetime.now()} Buy signal. New futures order. Price: {buy_price}')

                await entry_point_search(symbol, quantity, sell_signal)
            else:
                # order = client.futures_create_order(symbol=symbol, side='SELL', type='MARKET', quantity=quantity)

                sell_price = float(symbol_data.Close[-1])
                HISTORY.append((buy_price, sell_price, 100 - (100 * buy_price) / sell_price))
                apr = sum(order[2] for order in HISTORY)
                positive = sum(1 if order[1] - order[0] >= 0 else 0 for order in HISTORY)

                print(f'{datetime.datetime.now()} Sell signal. New futures order. Price: {sell_price}')
                print(f'Sum all orders: {apr}%\nCount orders: {len(HISTORY)}\nPositive orders: {positive}')

                await entry_point_search(symbol, quantity, buy_signal)


if __name__ == '__main__':
    client = Client(api_key=API_KEY, api_secret=SECRET)
    asyncio.run(print_1h_change())
    asyncio.run(entry_point_search('ETHBTC', 0))

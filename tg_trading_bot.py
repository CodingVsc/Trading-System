import logging
import os

import telebot
import pandas as pd
import pandas_ta as ta
from pybit.unified_trading import HTTP
from time import sleep


logging.basicConfig(level=logging.INFO)


BOT_TOKEN = str(os.environ.get('BOT_TOKEN'))
CHAT_ID = str(os.environ.get('CHAT_ID'))

bot = telebot.TeleBot(BOT_TOKEN)

session = HTTP(
    api_key=str(os.getenv("API_KEY")),
    api_secret=str(os.getenv("API_SECRET")),
)

last_signal = {}


def klines(symbol):
    try:
        resp = session.get_kline(
            category='linear',
            symbol=symbol,
            interval=15,
            limit=500
        )['result']['list']
        resp = pd.DataFrame(resp)
        resp.columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Turnover']
        resp = resp.astype(float)
        resp = resp[::-1]
        return resp
    except Exception as err:
        print(err)


def send_telegram_message(message):
    try:
        bot.send_message(chat_id=CHAT_ID, text=message)
    except Exception as e:
        print("Error sending message:", e)


def kst_ao_signal(symbol):
    kl = klines(symbol)
    ao = ta.ao(kl.High, kl.Low)
    ao_signal = None
    order_signal = None
    ao_time_up = None
    ao_time_down = None

    if ao.iloc[-2] < 0 < ao.iloc[-1]:
        ao_signal = 'up'
        ao_time_up = kl.iloc[-2].Time
    elif ao.iloc[-2] > 0 > ao.iloc[-1]:
        ao_signal = 'down'
        ao_time_down = kl.iloc[-2].Time

    if symbol in last_signal.keys():
        if (ao_signal == 'up'
                and ao_time_up != last_signal[symbol].get('ao_time_up')):
            order_signal = 'buy'
            last_signal[symbol] = {'ao_time_up': ao_time_up}
        elif (ao_signal == 'down'
                and ao_time_down != last_signal[symbol].get('ao_time_down')):
            order_signal = 'sell'
            last_signal[symbol] = {'ao_time_down': ao_time_down}

    elif symbol not in last_signal.keys():
        if ao_signal == 'up':
            order_signal = 'buy'
            last_signal[symbol] = {'ao_time_up': ao_time_up}
        elif ao_signal == 'down':
            order_signal = 'sell'
            last_signal[symbol] = {'ao_time_down': ao_time_down}

    return order_signal


def main():
    while True:
        my_tickers = ['BNBUSDT', 'GALAUSDT', 'SEIUSDT', 'UMAUSDT',
                      'ETHUSDT', 'BTCUSDT']
        for elem in my_tickers:
            signal = kst_ao_signal(elem)
            if signal == 'buy':
                print(f'Buying {elem}')
                send_telegram_message(f"🚀BUY signal detected for {elem}!"
                                      f'https://www.bybit.com/trade/usdt/{elem}'
                                     )

            elif signal == 'sell':
                print(f'Selling {elem}')
                send_telegram_message(f"🚀SELL signal detected for {elem}!"
                                      f'https://www.bybit.com/trade/usdt/{elem}'
                                      )
        sleep(30)


if __name__ == '__main__':
    main()

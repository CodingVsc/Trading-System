import os

import telebot
import pandas as pd
import pandas_ta as ta
from pybit.unified_trading import HTTP
import asyncio


BOT_TOKEN = str(os.environ.get('BOT_TOKEN'))
CHAT_ID = str(os.environ.get('CHAT_ID'))

bot = telebot.TeleBot(BOT_TOKEN)

session = HTTP(
    api_key=str(os.getenv("API_KEY")),
    api_secret=str(os.getenv("API_SECRET")),
)

last_signal = {}


async def klines(symbol):
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


async def send_telegram_message(message):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=message)
    except Exception as e:
        print("Error sending message:", e)


async def kst_ao_signal(symbol):
    kl = await klines(symbol)
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


async def process_trade(elem):
    while True:
        signal = await kst_ao_signal(elem)
        if signal == 'buy':
            await send_telegram_message(f"🚀BUY signal detected for {elem}!"
                                        f'https://www.bybit.com/trade/usdt/{elem}'
                                        )

        elif signal == 'sell':
            await send_telegram_message(f"🚀SELL signal detected for {elem}!"
                                        f'https://www.bybit.com/trade/usdt/{elem}'
                                        )
        await asyncio.sleep(3)


async def main():
    my_tickers = ['1000BONKUSDT', '1000FLOKIUSDT', '1000PEPEUSDT', 'ARUSDT',
                  'AVAXUSDT', 'BCHUSDT', 'BNBUSDT', 'BRETTUSDT', 'BTCUSDT', 'DEGENUSDT', 'DOGEUSDT',
                  'ENAUSDT', 'ETHUSDT', 'FETUSDT', 'FTMUSDT', 'GALAUSDT', 'HBARUSDT', 'JTOUSDT', 'LEVERUSDT',
                  'MATICUSDT', 'MERLUSDT', 'NEARUSDT', 'ONDOUSDT', 'OPUSDT', 'ORDIUSDT', 'PENDLEUSDT', 'POPCATUSDT',
                  'SEIUSDT', 'SHIB1000USDT', 'SOLUSDT', 'STXUSDT', 'SUIUSDT', 'TONUSDT', 'TRBUSDT',
                  'WIFUSDT', 'WLDUSDT', 'WUSDT', 'XRPUSDT', 'ZETAUSDT']
    tasks = []
    for elem in my_tickers:
        task = asyncio.create_task(process_trade(elem))
        tasks.append(task)

    await asyncio.gather(*tasks)


asyncio.run(main())
import os

from pybit.unified_trading import HTTP
import pandas as pd
import pandas_ta as ta
import asyncio


session = HTTP(
    api_key=str(os.getenv("API_KEY")),
    api_secret=str(os.getenv("API_SECRET")),
)

# Config:
tp = 0.005  # Take Profit +0.5%
sl = 0.005  # Stop Loss -0.5%
timeframe = 5  # 5 minutes
mode = 1  # 1 - Isolated, 0 - Cross
leverage = 1
qty = 6    # Amount of USDT for one order, 6 is min for ByBit
max_pos = 50    # Max current orders
# symbols = get_tickers()     # getting all symbols from the Bybit Derivatives
last_signal = {}   # data for signals


# Getting balance on Bybit Derivatrives Asset (in USDT)
async def get_balance():
    try:
        balance = session.get_wallet_balance(
            accountType="CONTRACT",
            coin="USDT"
        )['result']['list'][0]['coin'][0]['walletBalance']
        balance = float(balance)
        return balance
    except Exception as err:
        print(err)


# Getting all available symbols from Derivatives market (like 'BTCUSDT', 'XRPUSDT', etc)
async def get_tickers():
    try:
        tickers = session.get_tickers(category="linear")['result']['list']
        symbols = []
        for coin in tickers:
            if 'USDT' in coin['symbol'] and not 'USDC' in coin['symbol']:
                symbols.append(coin['symbol'])
        return symbols
    except Exception as err:
        print(err)


# Klines is the candles of some symbol (up to 1500 candles). Dataframe, last elem has [-1] index
async def klines(symbol):
    try:
        kl = session.get_kline(
            category='linear',
            symbol=symbol,
            interval=timeframe,
            limit=500
        )['result']['list']
        kl = pd.DataFrame(kl)
        kl.columns = ['Time', 'Open', 'High',
                      'Low', 'Close', 'Volume',
                      'Turnover']
        kl = kl.astype(float)
        kl = kl[::-1]
        return kl
    except Exception as err:
        print(err)


# Getting your current positions. It returns symbols list with opened positions
async def get_positions():
    try:
        positions = session.get_positions(
            category='linear',
            settleCoin='USDT'
        )['result']['list']
        return positions
    except Exception as err:
        print(err)


async def get_positions_symbol(elem):
    try:
        resp = session.get_positions(
            category='linear',
            settleCoin='USDT'
        )['result']['list']
        symbol_side = {}
        if len(resp) > 0:
            for i in resp:
                if i['symbol'] == elem:
                    symbol_side[elem] = i['side']
        return symbol_side
    except Exception as err:
        print(err)


async def get_rev_side(key):
    try:
        rev = session.get_positions(
            category='linear',
            settleCoin='USDT'
        )['result']['list'][0]
        rev['rev_side'] = ("Sell", "Buy")[rev['side'] == 'Sell']
        return rev.get(key)
    except Exception as err:
        print(err)


# Changing mode and leverage:
async def set_mode(symbol):
    try:
        margin_mode = session.switch_margin_mode(
            category='linear',
            symbol=symbol,
            tradeMode=mode,
            buyLeverage=str(leverage),
            sellLeverage=str(leverage)
        )
        print(margin_mode)
    except Exception as err:
        print(err)


# Getting number of decimal digits for price and qty
async def get_precisions(symbol):
    try:
        instruments_info = session.get_instruments_info(
            category='linear',
            symbol=symbol
        )['result']['list'][0]
        price = instruments_info['priceFilter']['tickSize']
        if '.' in price:
            price = len(price.split('.')[1])
        else:
            price = 0
        qty = instruments_info['lotSizeFilter']['qtyStep']
        if '.' in qty:
            qty = len(qty.split('.')[1])
        else:
            qty = 0

        return price, qty
    except Exception as err:
        print(err)


# Placing order with Market price. Placing TP and SL as well
async def place_order_market(symbol, side):
    price_precision = (await get_precisions(symbol))[0]
    qty_precision = (await get_precisions(symbol))[1]
    mark_price = session.get_tickers(
        category='linear',
        symbol=symbol
    )['result']['list'][0]['markPrice']
    mark_price = float(mark_price)
    print(f'Placing {side} order for {symbol}. Mark price: {mark_price}')
    order_qty = round(qty/mark_price, qty_precision)
    if side == 'buy':
        try:
            tp_price = round(mark_price + mark_price * tp, price_precision)
            sl_price = round(mark_price - mark_price * sl, price_precision)
            resp = session.place_order(
                category='linear',
                symbol=symbol,
                side='Buy',
                orderType='Market',
                qty=order_qty,
                takeProfit=tp_price,
                stopLoss=sl_price,
                tpTriggerBy='Market',
                slTriggerBy='Market'
            )
            print(resp)
        except Exception as err:
            print(err)

    if side == 'sell':
        try:
            tp_price = round(mark_price - mark_price * tp, price_precision)
            sl_price = round(mark_price + mark_price * sl, price_precision)
            resp = session.place_order(
                category='linear',
                symbol=symbol,
                side='Sell',
                orderType='Market',
                qty=order_qty,
                takeProfit=tp_price,
                stopLoss=sl_price,
                tpTriggerBy='Market',
                slTriggerBy='Market'
            )
            print(resp)
        except Exception as err:
            print(err)


# Trading strategy, time_lst consist of bars for last 30 minutes
async def kst_ao_signal(symbol):
    kl = await klines(symbol)
    ao = ta.ao(kl.High, kl.Low)
    kst = ta.kst(kl.Close)
    time_lst = [[-7, -6], [-6, -5],
                [-5, -4], [-4, -3],
                [-3, -2], [-2, -1]]
    ao_signal = None
    kst_signal = None
    order_signal = None
    ao_time_up = None
    kst_time_up = None
    kst_time_down = None
    ao_time_down = None

    for pair in time_lst:
        start_index = pair[0]
        end_index = pair[1]

        if ao.iloc[start_index] < 0 < ao.iloc[end_index]:
            ao_signal = 'up'
            ao_time_up = kl.iloc[start_index].Time
        elif ao.iloc[start_index] > 0 > ao.iloc[end_index]:
            ao_signal = 'down'
            ao_time_down = kl.iloc[start_index].Time
        if (kst.iloc[start_index].KST_10_15_20_30_10_10_10_15
                < kst.iloc[start_index].KSTs_9
                and kst.iloc[end_index].KST_10_15_20_30_10_10_10_15
                > kst.iloc[end_index].KSTs_9):
            kst_signal = 'up'
            kst_time_up = kl.iloc[start_index].Time
        elif (kst.iloc[start_index].KST_10_15_20_30_10_10_10_15
              > kst.iloc[start_index].KSTs_9
              and kst.iloc[end_index].KST_10_15_20_30_10_10_10_15
              < kst.iloc[end_index].KSTs_9):
            kst_signal = 'down'
            kst_time_down = kl.iloc[start_index].Time

    if symbol in last_signal.keys():
        if (ao_signal == 'up' and kst_signal == 'up'
                and kst_time_up != last_signal[symbol]['kst_time_up']
                and ao_time_up != last_signal[symbol]['ao_time_up']):
            order_signal = 'buy'
            last_signal[symbol] = {'ao_time_up': ao_time_up, 'kst_time_up': kst_time_up}
        elif (ao_signal == 'down' and kst_signal == 'down'
                and kst_time_down != last_signal[symbol]['kst_time_down']
                and ao_time_down != last_signal[symbol]['ao_time_down']):
            order_signal = 'sell'
            last_signal[symbol] = {'ao_time_down': ao_time_down, 'kst_time_down': kst_time_down}

    elif symbol not in last_signal.keys():
        if ao_signal == 'up' and kst_signal == 'up':
            order_signal = 'buy'
            last_signal[symbol] = {'ao_time_up': ao_time_up,
                                   'kst_time_up': kst_time_up}
        elif ao_signal == 'down' and kst_signal == 'down':
            order_signal = 'sell'
            last_signal[symbol] = {'ao_time_down': ao_time_down,
                                   'kst_time_down': kst_time_down}

    return order_signal


async def close_position(elem):
    args = dict(
        category='linear',
        symbol=elem,
        side=await get_rev_side('rev_side'),
        orderType="Market",
        qty=0.0,
        reduceOnly=True,
        closeOnTrigger=True,
    )
    try:
        session.place_order(**args)
        return 'Success'
    except Exception as e:
        return e


async def process_trade(elem):
    while True:
        signal = await kst_ao_signal(elem)
        tickers_side = await get_positions_symbol(elem)
        if signal == 'buy':
            if len(tickers_side) == 0:
                print(f'Found BUY signal for {elem}')
                await set_mode(elem)
                await place_order_market(elem, 'buy')
            elif len(tickers_side) > 0:
                if tickers_side[elem] == 'Sell':
                    await close_position(elem)
                elif tickers_side[elem] == 'Buy':
                    print(f'Waiting for sell signal for {elem}')
                    await wait_for_signal(elem, 'sell')
                    await close_position(elem)
        elif signal == 'sell':
            if len(tickers_side) == 0:
                print(f'Found SELL signal for {elem}')
                await set_mode(elem)
                await place_order_market(elem, 'sell')
            elif len(tickers_side) > 0:
                if tickers_side[elem] == 'Buy':
                    await close_position(elem)
                elif tickers_side[elem] == 'Sell':
                    print(f'Waiting for buy signal for {elem}')
                    await wait_for_signal(elem, 'buy')
                    await close_position(elem)
        await asyncio.sleep(3)


async def wait_for_signal(elem, target_signal):
    while await kst_ao_signal(elem) != target_signal:
        if await check_if_order_closed(elem):
            break
        await asyncio.sleep(3)


async def check_if_order_closed(elem):
    tickers_side = await get_positions_symbol(elem)
    if len(tickers_side) == 0:
        return True
    else:
        return False


async def main():
    my_tickers = ['BNBUSDT', 'DOGEUSDT', 'SEIUSDT']
    balance = await get_balance()
    pos = await get_positions()
    if balance is None:
        print('Cant connect to API')
    if balance is not None:
        balance = float(balance)
        print(f'Balance: {balance}')
        print(f'You have {len(pos)} positions: {pos}')

    tasks = []
    for elem in my_tickers:
        task = asyncio.create_task(process_trade(elem))
        tasks.append(task)

    await asyncio.gather(*tasks)


asyncio.run(main())

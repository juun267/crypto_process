import asyncio
import websockets
import json
from collections import defaultdict
from datetime import datetime
import time

from repositories import BaseRepository
from basic_snapshot import BasicSnapshot

order_data = defaultdict(lambda: {"SELL": 0.0, "BUY": 0.0})

BASE_URL = "wss://fstream.binance.com/ws/!forceOrder@arr"

async def process_message(message):
    global order_data

    data = json.loads(message)
    if "o" in data:
        event_data = data["o"]
        symbol = event_data['s']
        side = event_data['S']
        quantity = float(event_data['q'])

        if side == "SELL":
            order_data[symbol]["SELL"] += quantity
        elif side == "BUY":
            order_data[symbol]["BUY"] += quantity

async def print_results():
    global order_data
    repository = BaseRepository("liquidation")

    interval_time = 300

    while True:
        # Get current time and calculate seconds until next 5-minute mark
        now = datetime.now()
        # print(now)
        seconds_until_next_5_min = (interval_time - (now.minute * 60 + now.second) % interval_time)
        # print(seconds_until_next_5_min)

        # Wait until the next 5-minute mark
        await asyncio.sleep(seconds_until_next_5_min)

        # Save the data
        for symbol, data in order_data.items():
            sell_quantity = data['SELL']
            buy_quantity = data['BUY']

            repository.add({
                "exchange_id": 1,
                "symbol": symbol,
                "BuyLiq": buy_quantity,
                "SellLiq": sell_quantity,
                "timestamp": datetime.utcnow()
            })

        # Reset the order data after saving
        order_data = defaultdict(lambda: {"SELL": 0.0, "BUY": 0.0})

async def connect_websocket():
    while True:
        try:
            async with websockets.connect(BASE_URL) as websocket:
                print("Connected to WebSocket.")
                while True:
                    message = await websocket.recv()
                    await process_message(message)
        except (websockets.exceptions.ConnectionClosedError, websockets.exceptions.ConnectionClosedOK) as e:
            print(f"Connection closed: {e}. Reconnecting...")
        except Exception as e:
            print(f"Error receiving data: {e}. Reconnecting...")

        await asyncio.sleep(5)

async def main():
    await asyncio.gather(connect_websocket(), print_results())

if __name__ == "__main__":
    asyncio.run(main())

import asyncio
import websockets
import json
from collections import defaultdict
import time

# Store the quantities of each symbol's SELL and BUY orders
order_data = defaultdict(lambda: {"SELL": 0.0, "BUY": 0.0})

# WebSocket stream URL
BASE_URL = "wss://fstream.binance.com/ws/!forceOrder@arr"

# Function to process incoming WebSocket messages
async def process_message(message):
    global order_data

    # Parse the message
    data = json.loads(message)
    if "o" in data:
        event_data = data["o"]
        symbol = event_data['s']
        side = event_data['S']
        quantity = float(event_data['q'])

        # Print the event in the required JSON format
        print(json.dumps({
            "e": "forceOrder",  # Event Type
            "E": int(time.time() * 1000),  # Event Time (current time in ms)
            "o": {
                "s": symbol,  # Symbol
                "S": side,  # Side (SELL/BUY)
                "q": f"{quantity:.3f}"  # Quantity (formatted to 3 decimal places)
            }
        }, indent=4))

        # Update the SELL or BUY total for the symbol
        if side == "SELL":
            order_data[symbol]["SELL"] += quantity
        elif side == "BUY":
            order_data[symbol]["BUY"] += quantity

# Function to print the cumulative results every 5 minutes
async def print_results():
    start_time = time.time()

    while True:
        # Print the cumulative results every 5 minutes
        if time.time() - start_time >= 300:  # 5 minutes (300 seconds)
            for symbol, data in order_data.items():
                sell_quantity = data['SELL']
                buy_quantity = data['BUY']

                # Print the cumulative SELL and BUY in the required format
                print(f"{symbol.upper()} SELL {sell_quantity:.3f}")
                print(f"{symbol.upper()} BUY {buy_quantity:.3f}")

            # Reset the start time for the next 5-minute interval
            start_time = time.time()

        await asyncio.sleep(1)  # Check every second for the 5-minute interval

# Function to connect to the WebSocket with automatic reconnection
async def connect_websocket():
    while True:
        try:
            # Try to establish the WebSocket connection
            async with websockets.connect(BASE_URL) as websocket:
                print("Connected to WebSocket.")
                while True:
                    message = await websocket.recv()
                    await process_message(message)
        except (websockets.exceptions.ConnectionClosedError, websockets.exceptions.ConnectionClosedOK) as e:
            print(f"Connection closed: {e}. Reconnecting...")
        except Exception as e:
            print(f"Error receiving data: {e}. Reconnecting...")

        # Wait before trying to reconnect
        await asyncio.sleep(5)

# Main function to run everything
async def main():
    # Start the WebSocket connection and print results
    await asyncio.gather(connect_websocket(), print_results())

# Run the main function
if __name__ == "__main__":
    asyncio.run(main())

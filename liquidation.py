import asyncio
import json
import os
from datetime import datetime
from websockets import connect
from tabulate import tabulate
from IPython.display import display, clear_output

websocket_uri = "wss://fstream.binance.com/ws/!forceOrder@arr"
filename = "binance.csv"

if not os.path.isfile(filename):
    with open(filename, "w") as f:
        f.write(",".join(["symbol", "side", "order_type", "time_in_force",
                          "orginal_quantity", "price", "average_price", "order_status",
                          "order_last_filled_quantity", "order_filled_accumulated_quantity",
                          "order_trade_time"]) + "\n")


async def binance_liquidations(uri, filename):
    liquidations = []

    async for websocket in connect(uri):
        try:
            while True:
                msg = await websocket.recv()
                msg = json.loads(msg)["o"]
                msg = [str(x) for x in list(msg.values())]

                # Extract the order_trade_time and format it to datetime
                order_trade_time = int(msg[-1])  # Assuming it's the last element
                dt = datetime.fromtimestamp(order_trade_time / 1000)  # Convert milliseconds to seconds

                # Convert datetime to the desired format
                formatted_dt = dt.strftime('%Y-%m-%d %H:%M:%S')

                msg[-1] = formatted_dt  # Replace the original timestamp with the formatted datetime

                with open(filename, "a") as f:
                    f.write(",".join(msg) + "\n")

                liquidations.append(msg)

                # Update the graphical table
                clear_output(wait=True)
                display(tabulate(liquidations, headers=["Symbol", "Side", "Order Type", "Time in Force",
                                                       "Original Quantity", "Price", "Average Price",
                                                       "Order Status", "Order Last Filled Quantity",
                                                       "Order Filled Accumulated Quantity", "Order Trade Time"]))

        except Exception as e:
            print(e)
            continue


asyncio.run(binance_liquidations(websocket_uri, filename))

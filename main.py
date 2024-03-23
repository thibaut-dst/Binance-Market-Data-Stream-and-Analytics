import websocket
import json
import ssl
import pandas as pd
import threading

from config import spot_socket, futures_socket, book_streams, trade_streams


def on_message(ws, message, url): 
    """
    This function is called whenever a message is received from the WebSocket that streams book data.
    It parses the message as JSON and processes it using the `process_message` function.
    """
    message_data = json.loads(message)
    print("book data from", ws.url)
    #print(message_data) 
    process_message(message_data, url)  # Call process_message to handle the message

def on_trade_message(ws, message, url):
    """
    This function is called whenever a message is received from the WebSocket that streams trade data.
    It parses the message as JSON and processes it using the `process_message` function.
    """
    message_data = json.loads(message)
    print("trade data from", ws.url)
    #print(message_data)
    process_trade_message(message_data, url)

def on_error(ws, error):
    """
    Callback function for handling errors from the WebSocket
    """
    print(f"Error: {error}")

def process_message(message, url):
    """
    Process the message received from the WebSocket for book data
    """
    global book_data
    # Extract relevant data from the message
    timestamp = message["data"]["E"]
    symbol = message["data"]["s"]
    bids = message["data"]["b"]
    asks = message["data"]["a"]
    
    # Calculate the spread (best ask - best bid)
    #spread = float(asks[0][0]) - float(bids[0][0])
    
    # Process different levels of the order book
    for i, level_size in enumerate([1, 10, 25], start=1):
        
        # Extract bids and asks up to the specified level size

        bids_level = []
        for price in bids[:level_size]: #Iterate through the bid prices up to the specified level size
            #fetch and convert each bid price before appending to a bids_level list
            bids_level.append(float(price[0]))
        asks_level = []
        for price in asks[:level_size]:
            asks_level.append(float(price[0]))

        # Calculate average bid and ask prices
        avg_bid_price = sum(bids_level) / len(bids_level)
        avg_ask_price = sum(asks_level) / len(asks_level)
        average_spread = avg_ask_price - avg_bid_price
        
        type = "perp" if 'fstream.binance.com' in url else "spot"

        new_row = {
            "timestamp": timestamp,
            "instrument": f"{symbol}_{type}",
            "level_qty": int(level_size),
            "average_bid_price": avg_bid_price,
            "average_ask_price": avg_ask_price,
            "spread": average_spread
        }

        # Append data to the lists 
        book_data.append(new_row)

def process_trade_message(message, url):
    """
    Process the trade message received from the WebSocket for trade data
    """
    global trade_data

    # Extract relevant data from the trade message
    timestamp = message["data"]["T"]
    symbol = message["data"]["s"]
    price = float(message["data"]["p"])
    quantity = float(message["data"]["q"])
    
    volume = price * quantity
    
    type = "perp" if 'fstream.binance.com' in url else "spot"

    new_trade = {
        "timestamp": timestamp,
        "instrument": f"{symbol}_{type}",
        "volume": volume,
        "price": price
    }

    # Append data to the trade_data list
    trade_data.append(new_trade)

def start_websocket_thread(socket_url, on_message_callback):
    """
    Start a WebsocketApp instance and a new thread to run the WebSocket
    """
    ws = websocket.WebSocketApp(socket_url,
                                on_message=on_message_callback,
                                on_error=on_error)
    thread = threading.Thread(target=ws.run_forever, kwargs={"sslopt": {"cert_reqs": ssl.CERT_NONE}})
    thread.start()
    return ws, thread

def create_trade_report(df_book_data):
    """
    Generate a CSV report using the collected book and trade data
    Returns: Dataframe representing the trade report
    """
    global trade_data

    # Filter to process only the trade on the orderbook streaming period 
    period_start = df_book_data["timestamp"].iloc[0] 
    period_end = df_book_data["timestamp"].iloc[-1]
    filtered_trades = [trade for trade in trade_data if period_start <= trade['timestamp'] <= period_end]    

    df_trade = pd.DataFrame(filtered_trades) # Create DataFrames to store all spot and futures trading data        
    groupedByInstrument = df_trade.groupby("instrument")

    # Processing dataframe to get number of trade, volume, average volume, number of quotations, average spread for each level
    trade_nb = pd.Series(groupedByInstrument["volume"].count()) 
    volume = pd.Series(groupedByInstrument["volume"].sum())
    avg_volume = pd.Series(groupedByInstrument["volume"].mean())
    quotes_nb = pd.Series(groupedByInstrument["price"].nunique())
    
    trade_report = pd.concat([trade_nb, volume, avg_volume, quotes_nb], axis=1)
    trade_report.columns = ["trade_nb", "traded_volumes", "avg_volume", "unique_quotes"]
    
    avg_spread_level_1  = df[df['level_qty'] == 1].groupby('instrument')['spread'].mean()
    avg_spread_level_10 = df[df['level_qty'] == 10].groupby('instrument')['spread'].mean()
    avg_spread_level_25 = df[df['level_qty'] == 25].groupby('instrument')['spread'].mean()

    #add columns to the trade report
    trade_report['average_spread_level_1'] = avg_spread_level_1
    trade_report['average_spread_level_10'] = avg_spread_level_10
    trade_report['average_spread_level_25'] = avg_spread_level_25

    return trade_report


book_data = []
trade_data = []

if __name__ == "__main__":
    try:
        uri1 = spot_socket+trade_streams
        uri2 = futures_socket+trade_streams
        uri3 = spot_socket+book_streams
        uri4 = futures_socket+book_streams
        

        # Start WebSocket to stream trade data for spot trading  
        trade_spot_ws, trade_spot_thread = start_websocket_thread(uri1, lambda ws, message: on_trade_message(ws, message, uri1))
        # Start WebSocket to stream trade data for futures trading
        trade_futures_ws, trade_futures_thread = start_websocket_thread(uri2, lambda ws, message: on_trade_message(ws, message, uri2))
        # Start WebSocket to stream book data for spot trading
        book_spot_ws , book_spot_thread = start_websocket_thread(uri3, lambda ws, message: on_message(ws, message, uri3))
        # Start WebSocket to stream book data for futures trading
        book_futures_ws , book_futures_thread = start_websocket_thread(uri4, lambda ws, message: on_message(ws, message, uri4))


        # Wait for threads to finish when the connection is about to stop
        trade_spot_thread.join()
        trade_futures_thread.join()
        book_spot_thread.join()
        book_futures_thread.join()
    
    except KeyboardInterrupt:

        print("KeyboardInterrupt: Stopping WebSocket connections...")
        book_spot_ws.close()
        book_futures_ws.close()
        trade_spot_ws.close()
        trade_futures_ws.close()
        print("WebSocket connections closed.")


        df = pd.DataFrame(book_data) # Create DataFrames for spot and futures book data 
        df.to_csv("collected_data/book_data.csv", sep=',', encoding='utf-8', index=False)

        trade_report = create_trade_report(df)
        trade_report.to_csv("collected_data/trade_report.csv", sep=',', encoding='utf-8', index=True)
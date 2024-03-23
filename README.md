# Binance Streaming - Analytics Report Creation

### Overview
This project aims to collect and analyze streaming data from Binance exchange. 
It utilizes WebSocket connections to stream both trade and order book data for spot and futures markets.
The collected data is then processed and analyzed to generate insights into trading activity and market dynamics.

### Features
 - WebSocket connections to stream trade and order book data.
- Real-time processing of streamed data.
- Calculation of various analytics metrics such as trade count, traded volumes, average volume, unique quotes, and average spread for each level.
- Exporting of collected data and analytics reports to CSV files.


### Requirements
 - Python >= 3.8
- websocket-client library
- pandas library
    

### Usage
Run the main script to start streaming data:
Wait for the WebSocket connections to collect streaming data.
Press Ctrl + C to stop the script and close WebSocket connections.

You'll find the collected data and analytics reports in the collected_data directory under "trade_report.csv"
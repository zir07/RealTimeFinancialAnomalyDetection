# RealTimeFinancialAnomalyDetection
A real-time Pakistan Stock Exchange (PSX) financial market anomaly detection system using Kafka, Flink, and machine learning. 


Project Pipeline Flow:

PSX Data API → Kafka Producer → Kafka Topic → Flink → 
[Calculate indicators] → [Detect anomalies] → [Send alerts]


Java components:

StockPrice.java: The POJO for stock price data.

ParseStockDataFunction.java: A Flink MapFunction to parse incoming JSON strings into StockPrice objects.

AnomalyAlert.java: The POJO for anomaly alerts.

AnomalyDetectionFunction.java: A Flink ProcessWindowFunction to detect anomalies.

StockPriceConsumer.java: The main Flink application class for consuming stock prices and applying anomaly detection. Sets up a Kafka source, parses the data, applies a windowed process function for anomaly detection, and prints the results.


Python Component:

psx_kafka_producer.py: Sends data to Kafka

Detailed Description of Key Components and How They Work Together:

psx_kafka_producer.py (Python Producer):

This Python script will continuously fetch (or generate mock) PSX stock data, economic indicators, and news sentiment.

It serializes this data into JSON strings and sends them to the Kafka topics (psx-stock-prices, psx-economic-data, psx-news-sentiment).

StockPrice.java (Java POJO):

Defines the structure for the stock price data within the Flink application. Flink can efficiently work with POJOs.

ParseStockDataFunction.java (Flink MapFunction):

When Flink reads a JSON string from Kafka, this function will convert that string into a StockPrice Java object. This is crucial for structured processing in Flink.

AnomalyAlert.java (Java POJO):

Defines the structure for the output of the anomaly detection, including the symbol, message, current price, and expected price.

AnomalyDetectionFunction.java (Flink ProcessWindowFunction):

This is the core of the anomaly detection logic.

It receives a window of StockPrice objects for a given stock symbol.

It calculates the average price within that window.

It identifies prices that deviate significantly from the average (based on ANOMALY_THRESHOLD).

It also checks for high volatility based on the price range within the window.

When an anomaly is detected, it emits an AnomalyAlert object.

StockPriceConsumer.java (Main Flink Application):

Sets up the Flink execution environment.

Configures a KafkaSource: This connects to the psx-stock-prices Kafka topic.

Parses Kafka messages: Uses ParseStockDataFunction to convert JSON strings from Kafka into StockPrice objects.

Filters out parsing errors: Ensures only valid StockPrice objects proceed.

Keys the stream: keyBy(StockPrice::getSymbol) is critical for applying windowed operations per stock symbol.

Applies a Tumbling Window: window(TumblingEventTimeWindows.of(Time.minutes(5))) groups stock prices for each symbol into 5-minute intervals.

Applies AnomalyDetectionFunction: Processes each window to find anomalies.

Prints results: anomalies.print("ANOMALY ALERT") and stockStream.print("STOCK PRICE") will output the detected anomalies and raw stock prices to the Flink job manager's logs (and your console if running locally).

Contact me about this project: zaindy@gmail.com. License: GPLv3

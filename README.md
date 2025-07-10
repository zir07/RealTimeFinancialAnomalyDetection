# RealTimeFinancialAnomalyDetection
A real-time Pakistan Stock Exchange (PSX) financial market anomaly detection system using Kafka, Flink, and machine learning. 

## Windows Setup Instructions

Running this project on Windows requires a few specific configuration changes to avoid common issues with file paths and network binding. Follow these steps carefully after setting up your environment.

### 1. Configure Flink (`flink-conf.yaml`)

The default Flink configuration can cause issues on Windows. You must make two changes to the `C:\flink-1.17.1\conf\flink-conf.yaml` file.

**A. Bind to `127.0.0.1` instead of `localhost`**

This prevents issues with how Windows resolves `localhost`, which can cause Flink's components to fail to connect to each other.

Find and replace every occurrence of `localhost` with `127.0.0.1`. The keys to change will include:
- `jobmanager.rpc.address`
- `rest.address`
- `taskmanager.host`

**Example:**
```yaml
# Change this:
rest.address: localhost

# To this:
rest.address: 127.0.0.1
```

**B. Set a Static TaskManager Resource ID**

This prevents Flink from generating a temporary directory name with a colon (`:`) in it, which is an illegal character on Windows and will cause the TaskManager to crash.

Add the following line to the end of your `flink-conf.yaml` file:
```yaml
taskmanager.resource-id: taskmanager-1
```

### 2. Build the Project with Dependencies (Fat JAR)

The project needs to be packaged into a single JAR file that includes all of its dependencies (like the Jackson JSON library).

Ensure your `pom.xml` contains the `maven-shade-plugin`. If it doesn't, add the following inside the `<build>` section:

```xml
<plugins>
    <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <version>3.2.4</version>
        <executions>
            <execution>
                <phase>package</phase>
                <goals>
                    <goal>shade</goal>
                </goals>
                <configuration>
                    <transformers>
                        <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                            <mainClass>com.example.psx.jobs.Main</mainClass> <!-- Make sure this is your main class -->
                        </transformer>
                    </transformers>
                </configuration>
            </execution>
        </executions>
    </plugin>
</plugins>
```

Then, build the project from the root directory (`D:\RealTimeFinancialAnomalyDetection`) using Maven:
```bash
mvn clean package
```
This will create the required JAR file in the `target` directory.

### 3. Running the Full Stack

After starting Zookeeper, Kafka, and the Python producer, start the Flink cluster using the Cygwin terminal:
```bash
# In C:\flink-1.17.1
./bin/start-cluster.sh
```

### 4. Submitting the Flink Job

To submit the job, use the `flink run` command from a Cygwin terminal. It is crucial to use the **Windows-style path** for the JAR file.

```bash
# In C:\flink-1.17.1\bin
./flink.sh run "D:/RealTimeFinancialAnomalyDetection/target/psx-anomaly-detection-1.0-SNAPSHOT.jar"
```

### 5. Viewing the Output

The job's output (stock prices, alerts, etc.) is sent to the TaskManager's standard output log. You can watch it in real-time from a Cygwin terminal:
```bash
tail -f /cygdrive/c/flink-1.17.1/log/flink-*-taskexecutor-*.out
```


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
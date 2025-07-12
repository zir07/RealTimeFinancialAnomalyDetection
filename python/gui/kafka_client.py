
import json
import logging
from PySide6.QtCore import QObject, Signal, QThread
from kafka import KafkaConsumer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaConsumerWorker(QObject):
    """A worker that runs in a separate thread to consume Kafka messages."""
    stockDataReceived = Signal(dict)
    econDataReceived = Signal(dict)
    sentimentDataReceived = Signal(dict)
    anomalyReceived = Signal(dict)
    connectionStatus = Signal(str)

    def __init__(self, bootstrap_servers=['localhost:9092']):
        super().__init__()
        self.bootstrap_servers = bootstrap_servers
        self.running = False
        self.consumer = None

    def run(self):
        """The main consumer loop."""
        self.running = True
        try:
            self.consumer = KafkaConsumer(
                'psx-stock-prices', 
                'psx-economic-data', 
                'psx-news-sentiment',
                'anomaly-alerts', # Assuming this topic will be used for anomalies
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                auto_offset_reset='latest',
                consumer_timeout_ms=1000 # Unblock poll() after 1s
            )
            self.connectionStatus.emit("Connected to Kafka")
            logger.info("Kafka consumer connected.")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            self.connectionStatus.emit(f"Error: {e}")
            self.running = False
            return

        while self.running:
            logger.info("KafkaConsumerWorker: Polling for new messages...")
            try:
                for message in self.consumer:
                    logger.info(f"KafkaConsumerWorker: Received message: Topic={message.topic}, Key={message.key}, Value={message.value}")
                    if not self.running:
                        break
                    
                    topic = message.topic
                    data = message.value
                    key = message.key

                    if topic == 'psx-stock-prices':
                        logger.info(f"KafkaConsumerWorker: Emitting stockDataReceived for {data.get('symbol')}")
                        self.stockDataReceived.emit(data)
                    elif topic == 'psx-economic-data':
                        data['key'] = key # Add indicator name to data
                        self.econDataReceived.emit(data)
                    elif topic == 'psx-news-sentiment':
                        self.sentimentDataReceived.emit(data)
                    elif topic == 'anomaly-alerts':
                        self.anomalyReceived.emit(data)
            except Exception as e:
                logger.error(f"Error in consumer loop: {e}")
                # Continue trying to consume

        if self.consumer:
            self.consumer.close()
        logger.info("Kafka consumer stopped.")
        self.connectionStatus.emit("Disconnected")

    def stop(self):
        """Stops the consumer loop."""
        self.running = False

import json
import time
from kafka import KafkaProducer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AnomalyTrigger:
    def __init__(self, bootstrap_servers=['localhost:9092']):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )

    def send_anomaly(self, symbol, anomaly_score, anomaly_type="Price Spike", current_value=0.0):
        anomaly_data = {
            "timestamp": int(time.time() * 1000),
            "topic": "psx-stock-prices", # The topic where the anomaly was detected
            "key": symbol,
            "value": current_value,
            "anomaly_score": anomaly_score,
            "anomaly_type": anomaly_type,
            "description": f"Unusual {anomaly_type} detected for {symbol}"
        }
        try:
            future = self.producer.send('anomaly-alerts', value=anomaly_data, key=symbol)
            record_metadata = future.get(timeout=10)
            logger.info(f"Sent anomaly for {symbol} to {record_metadata.topic}[{record_metadata.partition}]")
            return True
        except Exception as e:
            logger.error(f"Error sending anomaly for {symbol} to Kafka: {e}")
            return False

    def close(self):
        self.producer.close()

if __name__ == "__main__":
    trigger = AnomalyTrigger()
    
    logger.info("Triggering anomalies...")
    # Trigger an anomaly for HBL
    trigger.send_anomaly("HBL", 0.95, "Sudden Price Drop", 150.25)
    time.sleep(1) # Give a small delay
    
    # Trigger an anomaly for UBL
    trigger.send_anomaly("UBL", 0.88, "Unusual Volume", 195.50)
    
    trigger.close()
    logger.info("Anomaly triggering complete.")

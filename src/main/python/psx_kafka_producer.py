import json
import time
import requests
from kafka import KafkaProducer
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup
import logging
import random

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PSXDataProducer:
    def __init__(self, bootstrap_servers=['localhost:9092']):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        
        # Major PSX companies to track
        self.major_stocks = [
            'HBL', 'UBL', 'OGDC', 'PSO', 'LUCK', 'ENGRO', 'HUBC', 'BAHL',
            'NESTLE', 'HABIBBANK', 'MCB', 'FATIMA', 'FFBL', 'KAPCO', 'MARI'
        ]
        
        # PSX API endpoints (these are sample endpoints - you'll need to verify actual APIs)
        self.psx_api_base = "https://dps.psx.com.pk"
        
    def scrape_psx_data_portal(self):
        """Scrape data from PSX Data Portal"""
        try:
            url = "https://dps.psx.com.pk/"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract index data (this is sample parsing - you'll need to adjust based on actual HTML structure)
            indices_data = []
            
            # Look for index values in the page
            # This is a simplified example - you'll need to inspect the actual HTML structure
            for element in soup.find_all(['div', 'span', 'td']):
                text = element.get_text(strip=True)
                if 'KSE-100' in text or any(stock in text for stock in self.major_stocks):
                    indices_data.append({
                        'source': 'psx_data_portal',
                        'data': text,
                        'timestamp': int(time.time() * 1000)
                    })
            
            return indices_data
            
        except Exception as e:
            logger.error(f"Error scraping PSX data portal: {e}")
            return []
    
    def fetch_from_psx_api(self):
        """Fetch data from PSX API (if available)"""
        try:
            # This is a sample API call - you'll need to find the actual PSX API endpoints
            # Some possible endpoints based on research:
            api_endpoints = [
                "/api/allindices",
                "/api/getindex",
                "/api/stocks"
            ]
            
            stock_data = []
            
            for endpoint in api_endpoints:
                try:
                    url = f"{self.psx_api_base}{endpoint}"
                    response = requests.get(url, timeout=10)
                    
                    if response.status_code == 200:
                        data = response.json()
                        stock_data.append({
                            'source': 'psx_api',
                            'endpoint': endpoint,
                            'data': data,
                            'timestamp': int(time.time() * 1000)
                        })
                        
                except Exception as e:
                    logger.warning(f"API endpoint {endpoint} failed: {e}")
                    continue
            
            return stock_data
            
        except Exception as e:
            logger.error(f"Error fetching from PSX API: {e}")
            return []
    
    def generate_mock_psx_data(self):
        """Generate mock PSX data for testing purposes"""
        
        mock_data = []
        
        for symbol in self.major_stocks:
            # Generate realistic PSX stock data
            base_price = {
                'HBL': 150.0, 'UBL': 180.0, 'OGDC': 85.0, 'PSO': 220.0,
                'LUCK': 650.0, 'ENGRO': 280.0, 'HUBC': 120.0, 'BAHL': 45.0,
                'NESTLE': 6500.0, 'HABIBBANK': 75.0, 'MCB': 200.0, 'FATIMA': 25.0,
                'FFBL': 18.0, 'KAPCO': 35.0, 'MARI': 1200.0
            }.get(symbol, 100.0)
            
            # Add some random variation
            price_change = random.uniform(-0.05, 0.05)  # ±5% change
            current_price = base_price * (1 + price_change)
            volume = random.randint(10000, 1000000)
            
            stock_data = {
                'symbol': symbol,
                'price': round(current_price, 2),
                'volume': volume,
                'change': round(current_price - base_price, 2),
                'change_percent': round(price_change * 100, 2),
                'timestamp': int(time.time() * 1000),
                'market': 'PSX',
                'currency': 'PKR',
                'source': 'mock_data'
            }
            
            mock_data.append(stock_data)
        
        return mock_data
    
    def fetch_economic_indicators(self):
        """Fetch Pakistani economic indicators"""
        try:
            # Sample economic data - you can integrate with actual APIs
            indicators = {
                'exchange_rate': {
                    'USD_PKR': random.uniform(280, 300),  # Sample PKR/USD rate
                    'timestamp': int(time.time() * 1000),
                    'source': 'mock_sbp'
                },
                'inflation_rate': {
                    'rate': random.uniform(20, 30),  # Sample inflation rate
                    'timestamp': int(time.time() * 1000),
                    'source': 'mock_pbs'
                },
                'interest_rate': {
                    'rate': random.uniform(15, 25),  # Sample policy rate
                    'timestamp': int(time.time() * 1000),
                    'source': 'mock_sbp'
                }
            }
            
            return indicators
            
        except Exception as e:
            logger.error(f"Error fetching economic indicators: {e}")
            return {}
    
    def send_to_kafka(self, topic, data, key=None):
        """Send data to Kafka topic"""
        try:
            future = self.producer.send(topic, value=data, key=key)
            record_metadata = future.get(timeout=10)
            logger.info(f"Sent to {topic}: {record_metadata.topic}[{record_metadata.partition}]")
            return True
        except Exception as e:
            logger.error(f"Error sending to Kafka: {e}")
            return False
    
    def run_producer(self, interval=5):
        """Main producer loop"""
        logger.info("Starting PSX Data Producer...")
        
        while True:
            try:
                # Fetch stock data from multiple sources
                logger.info("Fetching PSX stock data...")
                
                # Try real data sources first
                psx_portal_data = self.scrape_psx_data_portal()
                psx_api_data = self.fetch_from_psx_api()
                
                # If no real data, use mock data for testing
                if not psx_portal_data and not psx_api_data:
                    logger.info("Using mock data for testing...")
                    mock_data = self.generate_mock_psx_data()
                    
                    for stock in mock_data:
                        self.send_to_kafka('psx-stock-prices', stock, key=stock['symbol'])
                
                # Send real data if available
                for data in psx_portal_data:
                    self.send_to_kafka('psx-stock-prices', data)
                
                for data in psx_api_data:
                    self.send_to_kafka('psx-stock-prices', data)
                
                # Fetch and send economic indicators
                logger.info("Fetching economic indicators...")
                economic_data = self.fetch_economic_indicators()
                
                for indicator, data in economic_data.items():
                    self.send_to_kafka('psx-economic-data', data, key=indicator)
                
                # Sample news sentiment (you can integrate with news APIs)
                news_sentiment = {
                    'headline': 'Sample PSX market news',
                    'sentiment': random.choice(['positive', 'negative', 'neutral']),
                    'score': random.uniform(-1, 1),
                    'timestamp': int(time.time() * 1000),
                    'source': 'mock_news'
                }
                
                self.send_to_kafka('psx-news-sentiment', news_sentiment)
                
                logger.info(f"Data sent successfully. Waiting {interval} seconds...")
                time.sleep(interval)
                
            except KeyboardInterrupt:
                logger.info("Producer stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in producer loop: {e}")
                time.sleep(interval)
    
    def close(self):
        """Close the producer"""
        self.producer.close()

if __name__ == "__main__":
    # Create and run the producer
    producer = PSXDataProducer()
    
    try:
        producer.run_producer(interval=10)  # Send data every 10 seconds
    except KeyboardInterrupt:
        print("\nShutting down producer...")
    finally:
        producer.close()
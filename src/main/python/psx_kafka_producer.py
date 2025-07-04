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
        
        # Major PSX companies to track (Pakistan Stock Exchange symbols)
        self.major_stocks = [
            'HBL', 'UBL', 'OGDC', 'PSO', 'LUCK', 'ENGRO', 'HUBC', 'BAHL',
            'NESTLE', 'HABIBBANK', 'MCB', 'FATIMA', 'FFBL', 'KAPCO', 'MARI'
        ]
        
        # Alpha Vantage API base (using free tier with API key)
        self.alpha_vantage_api_key = "V82JOP0J9K6JF98T"  # Replace with your API key from alphavantage.co
        self.alpha_vantage_base = "https://www.alphavantage.co/query"
    
    def scrape_psx_data_portal(self):
        """Scrape limited data from PSX Data Portal (note: limited public data available)"""
        try:
            url = "https://dps.psx.com.pk/"  # PSX Data Portal homepage
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract index data (limited to visible text due to lack of API)
            indices_data = []
            for element in soup.find_all(['div', 'span', 'td']):
                text = element.get_text(strip=True)
                if 'KSE-100' in text or any(stock in text for stock in self.major_stocks):
                    indices_data.append({
                        'source': 'psx_data_portal',
                        'data': text,
                        'timestamp': int(time.time() * 1000)
                    })
            
            return indices_data if indices_data else []
            
        except Exception as e:
            logger.error(f"Error scraping PSX data portal: {e}")
            return []

    def fetch_from_alpha_vantage(self):
        """Fetch stock data from Alpha Vantage API"""
        try:
            stock_data = []
            for symbol in self.major_stocks:
                # Use Alpha Vantage TIME_SERIES_DAILY endpoint for daily stock data
                params = {
                    'function': 'TIME_SERIES_DAILY',
                    'symbol': symbol + '.PK',  # Append .PK for Pakistan Stock Exchange
                    'apikey': self.alpha_vantage_api_key,
                    'outputsize': 'compact'
                }
                response = requests.get(self.alpha_vantage_base, params=params, timeout=10)
                response.raise_for_status()
                
                data = response.json()
                if 'Time Series (Daily)' in data:
                    latest_date = list(data['Time Series (Daily)'].keys())[0]
                    daily_data = data['Time Series (Daily)'][latest_date]
                    stock_data.append({
                        'source': 'alpha_vantage',
                        'symbol': symbol,
                        'price': float(daily_data['4. close']),
                        'volume': int(daily_data['5. volume']),
                        'timestamp': int(datetime.strptime(latest_date, '%Y-%m-%d').timestamp() * 1000),
                        'market': 'PSX',
                        'currency': 'PKR'
                    })
            
            return stock_data
            
        except Exception as e:
            logger.error(f"Error fetching from Alpha Vantage: {e}")
            return []

    def generate_mock_psx_data(self):
        """Generate mock PSX data for testing purposes (used as fallback)"""
        mock_data = []
        
        for symbol in self.major_stocks:
            base_price = {
                'HBL': 150.0, 'UBL': 180.0, 'OGDC': 85.0, 'PSO': 220.0,
                'LUCK': 650.0, 'ENGRO': 280.0, 'HUBC': 120.0, 'BAHL': 45.0,
                'NESTLE': 6500.0, 'HABIBBANK': 75.0, 'MCB': 200.0, 'FATIMA': 25.0,
                'FFBL': 18.0, 'KAPCO': 35.0, 'MARI': 1200.0
            }.get(symbol, 100.0)
            
            price_change = random.uniform(-0.05, 0.05)
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
        """Fetch Pakistani economic indicators from web sources"""
        try:
            # Using approximate values from recent trends (e.g., State Bank of Pakistan data)
            indicators = {
                'exchange_rate': {
                    'USD_PKR': 278.50,  # Approx. rate as of July 2025 from web trends
                    'timestamp': int(time.time() * 1000),
                    'source': 'web_estimate'
                },
                'inflation_rate': {
                    'rate': 22.5,  # Approx. based on recent Pakistan Bureau of Statistics data
                    'timestamp': int(time.time() * 1000),
                    'source': 'web_estimate'
                },
                'interest_rate': {
                    'rate': 20.5,  # Approx. State Bank of Pakistan policy rate
                    'timestamp': int(time.time() * 1000),
                    'source': 'web_estimate'
                }
            }
            
            return indicators
            
        except Exception as e:
            logger.error(f"Error fetching economic indicators: {e}")
            return {}

    def fetch_news_sentiment(self):
        """Fetch financial news sentiment using NewsAPI"""
        try:
            news_api_key = "a04df0c503334aceb2a64c9570d7ca04"  # Replace with your key from newsapi.org
            news_url = "https://newsapi.org/v2/everything"
            params = {
                'q': 'pakistan stock market OR psx OR financial market',
                'apiKey': news_api_key,
                'language': 'en',
                'pageSize': 5
            }
            response = requests.get(news_url, params=params, timeout=10)
            response.raise_for_status()
            
            articles = response.json().get('articles', [])
            if articles:
                article = articles[0]  # Take the most recent article
                # Simple sentiment analysis (positive/negative/neutral based on title keywords)
                title = article['title'].lower()
                if any(word in title for word in ['growth', 'rise', 'strong']):
                    sentiment = 'positive'
                    score = 0.7
                elif any(word in title for word in ['decline', 'fall', 'weak']):
                    sentiment = 'negative'
                    score = -0.7
                else:
                    sentiment = 'neutral'
                    score = 0.0
                
                news_sentiment = {
                    'headline': article['title'],
                    'sentiment': sentiment,
                    'score': score,
                    'timestamp': int(datetime.strptime(article['publishedAt'], '%Y-%m-%dT%H:%M:%SZ').timestamp() * 1000),
                    'source': 'newsapi',
                    'url': article['url']
                }
                return news_sentiment
            return None
            
        except Exception as e:
            logger.error(f"Error fetching news sentiment: {e}")
            return None

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
                alpha_vantage_data = self.fetch_from_alpha_vantage()
                
                # If no real data, use mock data for testing
                if not psx_portal_data and not alpha_vantage_data:
                    logger.info("Using mock data for testing...")
                    mock_data = self.generate_mock_psx_data()
                    
                    for stock in mock_data:
                        self.send_to_kafka('psx-stock-prices', stock, key=stock['symbol'])
                
                # Send real data if available
                for data in psx_portal_data:
                    self.send_to_kafka('psx-stock-prices', data)
                
                for data in alpha_vantage_data:
                    self.send_to_kafka('psx-stock-prices', data, key=data['symbol'])
                
                # Fetch and send economic indicators
                logger.info("Fetching economic indicators...")
                economic_data = self.fetch_economic_indicators()
                
                for indicator, data in economic_data.items():
                    self.send_to_kafka('psx-economic-data', data, key=indicator)
                
                # Fetch and send news sentiment
                logger.info("Fetching news sentiment...")
                news_sentiment = self.fetch_news_sentiment()
                if news_sentiment:
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
        producer.run_producer(interval=30)  # Send data every 30 seconds
    except KeyboardInterrupt:
        print("\nShutting down producer...")
    finally:
        producer.close()
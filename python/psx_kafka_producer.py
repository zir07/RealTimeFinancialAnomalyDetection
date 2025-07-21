import json
import time
import requests
from kafka import KafkaProducer
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup
import logging
import re
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
        # Major PSX companies to track (Pakistan Stock Exchange symbols)
        # This list should be regularly updated to reflect current market listings.
        self.major_stocks = [
            'HBL', 'UBL', 'OGDC', 'PSO', 'LUCK', 'HUBC', 'BAHL',
            'NESTLE', 'MCB', 'KAPCO', 'MARI', 'PTC', 'PAKT', 'SSGC', 'TGL'
        ]
        
        self.psx_terminal_base = "https://psxterminal.com/api"
    
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

    def fetch_from_psx_terminal(self):
        """Fetch stock data from PSX Terminal API"""
        try:
            stock_data = []
            for symbol in self.major_stocks:
                response = requests.get(f"{self.psx_terminal_base}/yields/{symbol}", timeout=10)
                response.raise_for_status()
                
                data = response.json()
                if data.get('success') and data.get('data') and 'price' in data['data']:
                    yield_data = data['data']
                    stock_data.append({
                        'source': 'psx_terminal',
                        'symbol': symbol,
                        'price': float(yield_data['price']),
                        'volume': int(yield_data.get('volume30Avg', 0)),
                        'timestamp': int(time.time() * 1000),
                        'market': 'PSX',
                        'currency': 'PKR'
                    })
                else:
                    logger.warning(f"Could not find price data for {symbol} in PSX Terminal response.")

            return stock_data
            
        except Exception as e:
            logger.error(f"Error fetching from PSX Terminal: {e}")
            return []

    def generate_mock_psx_data(self):
        """Generate mock PSX data for testing purposes (used as fallback)"""
        mock_data = []
        
        for symbol in self.major_stocks:
            base_price = {
                'HBL': 160.0, 'UBL': 190.0, 'OGDC': 90.0, 'PSO': 230.0,
                'LUCK': 680.0, 'HUBC': 130.0, 'BAHL': 50.0,
                'NESTLE': 6800.0, 'MCB': 210.0, 'KAPCO': 40.0, 'MARI': 1250.0,
                'PTCL': 15.0, 'PAKT': 1500.0, 'SSGC': 12.0, 'TGL': 80.0
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
    
    def _scrape_dawn_economic_data(self):
        """Scrape economic indicators from Dawn.com"""
        scraped_data = {}
        try:
            url = "https://www.dawn.com/business"  # Updated URL for economy section
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            # Attempt to find exchange rate (USD to PKR)
            # This is highly dependent on Dawn.com's HTML structure and may need adjustment
            exchange_rate_element = soup.find(lambda tag: "USD to PKR" in tag.get_text() or "Dollar" in tag.get_text())
            if exchange_rate_element:
                # Look for a number in the vicinity of the text
                match = re.search(r'\d+\.?\d*', exchange_rate_element.get_text())
                if match:
                    scraped_data['exchange_rate'] = {
                        'USD_PKR': float(match.group(0)),
                        'timestamp': int(time.time() * 1000),
                        'source': 'dawn.com'
                    }

            # Attempt to find inflation rate
            inflation_element = soup.find(lambda tag: "inflation" in tag.get_text() or "CPI" in tag.get_text())
            if inflation_element:
                match = re.search(r'\d+\.?\d*%', inflation_element.get_text())
                if match:
                    scraped_data['inflation_rate'] = {
                        'rate': float(match.group(0).replace('%', '')),
                        'timestamp': int(time.time() * 1000),
                        'source': 'dawn.com'
                    }

            # Attempt to find interest rate (policy rate)
            interest_rate_element = soup.find(lambda tag: "interest rate" in tag.get_text() or "policy rate" in tag.get_text())
            if interest_rate_element:
                match = re.search(r'\d+\.?\d*%', interest_rate_element.get_text())
                if match:
                    scraped_data['interest_rate'] = {
                        'rate': float(match.group(0).replace('%', '')),
                        'timestamp': int(time.time() * 1000),
                        'source': 'dawn.com'
                    }

        except Exception as e:
            logger.error(f"Error scraping Dawn.com for economic indicators: {e}")
        return scraped_data

    def fetch_economic_indicators(self):
        """Fetch Pakistani economic indicators from web sources"""
        economic_data = {}
        try:
            # Try to scrape from Dawn.com first
            scraped_data = self._scrape_dawn_economic_data()
            if scraped_data:
                economic_data = scraped_data
                logger.info("Successfully scraped economic data from Dawn.com")
            else:
                logger.warning("Dawn.com scraping failed or found no data, using approximate values.")
                # Fallback to approximate values if scraping fails
                economic_data = {
                    'exchange_rate': {
                        'USD_PKR': 285.0,  # Updated based on user input
                        'timestamp': int(time.time() * 1000),
                        'source': 'user_input'
                    },
                    'inflation_rate': {
                        'rate': 3.2,  # Updated based on user input
                        'timestamp': int(time.time() * 1000),
                        'source': 'user_input'
                    },
                    'interest_rate': {
                        'rate': 11.0,  # Updated based on user input
                        'timestamp': int(time.time() * 1000),
                        'source': 'user_input'
                    }
                }
            
            return economic_data
            
        except Exception as e:
            logger.error(f"Error in fetch_economic_indicators: {e}")
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
            
            data = response.json()
            articles = data.get('articles', [])
            
            if not articles:
                logger.warning(f"NewsAPI returned no articles for query. Response: {data}")
                return None

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
                
                # Fetch stock data from PSX Terminal
                psx_terminal_data = self.fetch_from_psx_terminal()

                stock_data_sent = False
                if psx_terminal_data:
                    logger.info("Successfully fetched live data from PSX Terminal.")
                    for data in psx_terminal_data:
                        self.send_to_kafka('psx-stock-prices', data, key=data['symbol'])
                    stock_data_sent = True

                if not stock_data_sent:
                    logger.warning("Failed to fetch live data from PSX Terminal, falling back to mock data.")
                    mock_data = self.generate_mock_psx_data()
                    for stock in mock_data:
                        self.send_to_kafka('psx-stock-prices', stock, key=stock['symbol'])
                
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
        producer.run_producer(interval=10)  # Send data every 10 seconds
    except KeyboardInterrupt:
        print("\nShutting down producer...")
    finally:
        producer.close()
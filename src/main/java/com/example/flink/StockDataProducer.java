package com.example.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class StockDataProducer {
    
    private static final String[] SYMBOLS = {"AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "META", "NVDA", "NFLX"};
    private static final String TOPIC = "psx-stock-prices";
    
    public static void main(String[] args) throws Exception {
        // Configure Kafka producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ObjectMapper objectMapper = new ObjectMapper();
        Random random = new Random();
        
        System.out.println("Starting to produce stock price data...");
        
        try {
            // Base prices for each symbol
            double[] basePrices = {150.0, 2500.0, 300.0, 3200.0, 800.0, 320.0, 450.0, 550.0};
            
            for (int i = 0; i < 1000; i++) {
                for (int j = 0; j < SYMBOLS.length; j++) {
                    String symbol = SYMBOLS[j];
                    double basePrice = basePrices[j];
                    
                    // Generate price with small random variation
                    double variation = (random.nextGaussian() * 0.02); // 2% standard deviation
                    double price = basePrice * (1 + variation);
                    
                    // Occasionally inject anomalies
                    if (random.nextDouble() < 0.05) { // 5% chance of anomaly
                        price = basePrice * (1 + (random.nextGaussian() * 0.25)); // 25% deviation
                        System.out.println("Injecting anomaly for " + symbol + ": " + price);
                    }
                    
                    // Create JSON message
                    StockPrice stockPrice = new StockPrice(
                        symbol,
                        Math.round(price * 100.0) / 100.0, // Round to 2 decimal places
                        System.currentTimeMillis(),
                        random.nextDouble() * 1000000 // Random volume
                    );
                    
                    String jsonMessage = objectMapper.writeValueAsString(stockPrice);
                    
                    // Send to Kafka
                    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, symbol, jsonMessage);
                    producer.send(record);
                    
                    System.out.println("Sent: " + jsonMessage);
                }
                
                // Wait a bit between batches
                Thread.sleep(2000); // 2 seconds
            }
            
        } finally {
            producer.close();
        }
        
        System.out.println("Finished producing data");
    }
}
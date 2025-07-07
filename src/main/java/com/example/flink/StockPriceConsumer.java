package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Properties;

public class StockPriceConsumer {
    
    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Enable checkpointing for fault tolerance
        env.enableCheckpointing(5000);
        
        // Configure Kafka properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-stock-consumer");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("enable.auto.commit", "false");
        
        // Create Kafka source (newer API, more robust than FlinkKafkaConsumer)
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("psx-stock-prices")
                .setGroupId("flink-stock-consumer")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        
        // Create the data stream from Kafka
        DataStream<String> kafkaStream = env.fromSource(
                source,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                        .withTimestampAssigner((element, recordTimestamp) -> System.currentTimeMillis()),
                "Kafka Source"
        );
        
        // Process the stream using your original code structure
        DataStream<StockPrice> stockStream = kafkaStream
                .map(new ParseStockDataFunction())
                .name("Parse Stock Data")
                .filter(stockPrice -> !stockPrice.getSymbol().equals("UNKNOWN")) // Filter out parsing errors
                .keyBy(StockPrice::getSymbol);
        
        // Apply your anomaly detection (modified for newer Flink API)
        DataStream<AnomalyAlert> anomalies = stockStream
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .process(new AnomalyDetectionFunction())
                .name("Anomaly Detection");
        
        // Output results
        anomalies.print("ANOMALY ALERT");
        
        // Also print regular stock prices for monitoring
        stockStream.print("STOCK PRICE");
        
        // Execute the job
        env.execute("PSX Stock Price Anomaly Detection Consumer");
    }
}
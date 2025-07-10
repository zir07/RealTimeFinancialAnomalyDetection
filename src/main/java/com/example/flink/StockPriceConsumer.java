package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Properties;

public class StockPriceConsumer {

    private static final OutputTag<String> economicDataTag = new OutputTag<String>("economic-data") {};
    private static final OutputTag<String> newsSentimentTag = new OutputTag<String>("news-sentiment") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-stock-consumer");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(properties.getProperty("bootstrap.servers"))
                .setTopics("psx-stock-prices", "psx-economic-data", "psx-news-sentiment")
                .setGroupId(properties.getProperty("group.id"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        SingleOutputStreamOperator<StockPrice> parsedStream = kafkaStream.process(new DataRouter());

        WatermarkStrategy<StockPrice> watermarkStrategy = WatermarkStrategy
                .<StockPrice>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp());

        SingleOutputStreamOperator<StockPrice> stockPriceStream = parsedStream.assignTimestampsAndWatermarks(watermarkStrategy);

        DataStream<String> economicDataStream = stockPriceStream.getSideOutput(economicDataTag);
        DataStream<String> newsSentimentStream = stockPriceStream.getSideOutput(newsSentimentTag);

        KeyedStream<StockPrice, String> keyedStockStream = stockPriceStream
                .filter(stockPrice -> !stockPrice.getSymbol().equals("UNKNOWN"))
                .name("Filter Invalid Stock Data")
                .keyBy(StockPrice::getSymbol);

        DataStream<AnomalyAlert> anomalies = keyedStockStream
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .process(new AnomalyDetectionFunction())
                .name("Anomaly Detection");

        anomalies.print("ANOMALY ALERT");
        stockPriceStream.print("STOCK PRICE");
        economicDataStream.print("ECONOMIC DATA");
        newsSentimentStream.print("NEWS SENTIMENT");

        env.execute("PSX Multi-Topic Anomaly Detection Consumer");
    }

    public static class DataRouter extends ProcessFunction<String, StockPrice> {
        private transient ObjectMapper objectMapper;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            objectMapper = new ObjectMapper();
        }

        @Override
        public void processElement(String value, Context ctx, Collector<StockPrice> out) throws Exception {
            try {
                JsonNode jsonNode = objectMapper.readTree(value);
                if (jsonNode.has("source")) {
                    String source = jsonNode.get("source").asText();
                    switch (source) {
                        case "alpha_vantage":
                        case "mock_data":
                        case "psx_data_portal":
                            StockPrice stockPrice = new StockPrice();
                            stockPrice.setSymbol(jsonNode.get("symbol").asText("UNKNOWN"));
                            stockPrice.setPrice(jsonNode.get("price").asDouble(0.0));
                            stockPrice.setTimestamp(jsonNode.get("timestamp").asLong(0L));
                            stockPrice.setVolume(jsonNode.get("volume").asInt(0));
                            out.collect(stockPrice);
                            break;
                        case "web_estimate":
                            ctx.output(economicDataTag, value);
                            break;
                        case "newsapi":
                            ctx.output(newsSentimentTag, value);
                            break;
                    }
                }
            } catch (Exception e) {
                // Log or handle parsing errors
            }
        }
    }
}

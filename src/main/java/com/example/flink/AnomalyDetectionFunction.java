package com.example.flink;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class AnomalyDetectionFunction extends ProcessWindowFunction<StockPrice, AnomalyAlert, String, TimeWindow> {
    
    private static final double ANOMALY_THRESHOLD = 0.15; // 15% deviation threshold

    @Override
    public void process(String symbol, Context context, Iterable<StockPrice> elements, Collector<AnomalyAlert> out) throws Exception {
        List<StockPrice> stockPrices = new ArrayList<>();
        
        // Collect all prices in the window
        for (StockPrice price : elements) {
            stockPrices.add(price);
        }
        
        if (stockPrices.isEmpty()) {
            return;
        }
        
        // Calculate average price
        double sum = 0.0;
        for (StockPrice price : stockPrices) {
            sum += price.getPrice();
        }
        double average = sum / stockPrices.size();
        
        // Check for anomalies
        for (StockPrice price : stockPrices) {
            double deviation = Math.abs(price.getPrice() - average) / average;
            
            if (deviation > ANOMALY_THRESHOLD) {
                String alertMessage = String.format(
                    "Price anomaly detected for %s: Current=%.2f, Average=%.2f, Deviation=%.2f%%",
                    symbol, price.getPrice(), average, deviation * 100
                );
                
                AnomalyAlert alert = new AnomalyAlert(
                    symbol,
                    alertMessage,
                    price.getTimestamp(),
                    price.getPrice(),
                    average
                );
                
                out.collect(alert);
            }
        }
        
        // Additional check for overall window statistics
        if (stockPrices.size() > 1) {
            double min = stockPrices.stream().mapToDouble(StockPrice::getPrice).min().orElse(0.0);
            double max = stockPrices.stream().mapToDouble(StockPrice::getPrice).max().orElse(0.0);
            double range = max - min;
            double rangePercent = (range / average) * 100;
            
            if (rangePercent > 25.0) { // If range is more than 25% of average
                String alertMessage = String.format(
                    "High volatility detected for %s: Range=%.2f%% (Min=%.2f, Max=%.2f, Avg=%.2f)",
                    symbol, rangePercent, min, max, average
                );
                
                AnomalyAlert alert = new AnomalyAlert(
                    symbol,
                    alertMessage,
                    context.window().getEnd(),
                    max,
                    average
                );
                
                out.collect(alert);
            }
        }
    }
}
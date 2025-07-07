package com.example.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;

public class ParseStockDataFunction implements MapFunction<String, StockPrice> {
    
    private transient ObjectMapper objectMapper;

    @Override
    public StockPrice map(String jsonString) throws Exception {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }

        try {
            JsonNode rootNode = objectMapper.readTree(jsonString);
            
            StockPrice stockPrice = new StockPrice();
            stockPrice.setSymbol(rootNode.get("symbol").asText());
            stockPrice.setPrice(rootNode.get("price").asDouble());
            stockPrice.setTimestamp(rootNode.get("timestamp").asLong());
            
            // Handle volume field if it exists, otherwise default to 0
            if (rootNode.has("volume")) {
                stockPrice.setVolume(rootNode.get("volume").asDouble());
            } else {
                stockPrice.setVolume(0.0);
            }
            
            return stockPrice;
        } catch (Exception e) {
            // Log error and return null or default object
            System.err.println("Error parsing JSON: " + jsonString + " - " + e.getMessage());
            
            // Return a default StockPrice object or rethrow exception
            StockPrice errorStock = new StockPrice();
            errorStock.setSymbol("UNKNOWN");
            errorStock.setPrice(0.0);
            errorStock.setTimestamp(System.currentTimeMillis());
            errorStock.setVolume(0.0);
            return errorStock;
        }
    }
}
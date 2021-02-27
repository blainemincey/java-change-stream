package com.mongodb;

import com.github.javafaker.Faker;
import org.bson.types.ObjectId;
import java.util.Date;

/**
 *
 */
public class Stock {
    private ObjectId id;
    private String stockTicker;
    private double price;
    private java.util.Date timeStamp;

    /**
     *
     */
    public Stock() {
        Faker faker = new Faker();

        this.setStockTicker(faker.stock().nyseSymbol());
        this.setPrice(Double.valueOf(faker.commerce().price()));
        this.setTimeStamp(new java.util.Date());
    }

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public String getStockTicker() {
        return stockTicker;
    }

    public void setStockTicker(String stockTicker) {
        this.stockTicker = stockTicker;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public Date getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Date timeStamp) {
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "Stock{" +
                "id=" + id +
                ", stockTicker='" + stockTicker + '\'' +
                ", price=" + price +
                ", timeStamp=" + timeStamp +
                '}';
    }
}

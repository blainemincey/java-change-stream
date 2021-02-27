package com.mongodb;

import com.mongodb.client.MongoClients;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.github.cdimascio.dotenv.Dotenv;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import java.util.concurrent.TimeUnit;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;


/**
 * Generates random Stocks and prices.
 */
public class StockTicker {

    // MongoDB Variables
    private String mongodbUri;
    private String mongodbDatabaseName;
    private String mongodbCollection;
    private MongoDatabase mongodbDatabase;

    /**
     *
     */
    public StockTicker() {
        this.init();
    }

    /**
     *
     */
    private void init() {
        System.out.println("Initialize properties and database.");

        Dotenv dotenv = Dotenv.configure().filename("application.properties").load();
        this.mongodbUri = dotenv.get("MONGODB_URI");
        this.mongodbDatabaseName = dotenv.get("MONGODB_DATABASE_NAME");
        this.mongodbCollection = dotenv.get("MONGODB_COLLECTION");

        // Init db
        ConnectionString connectionString = new ConnectionString(this.mongodbUri);

        CodecRegistry pojoCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        MongoClientSettings settings = MongoClientSettings.builder()
                .codecRegistry(pojoCodecRegistry)
                .applyConnectionString(connectionString)
                .build();

        MongoClient mongoClient = MongoClients.create(settings);
        this.mongodbDatabase = mongoClient.getDatabase(this.mongodbDatabaseName);

        this.generateStocks();
    }

    /**
     *
     */
    private void generateStocks() {
        System.out.println("Begin generating random stocks.");

        MongoCollection<Stock> mongoCollection
                = this.mongodbDatabase.getCollection(this.mongodbCollection, Stock.class);

        int totalInserted = 0;

        // Simply control-c to exit
        // For every 10 records sleep for 2 seconds
        while(true) {

            int count = 0;

            while(count < 10) {
                Stock stock = new Stock();

                // insert stock
                mongoCollection.insertOne(stock);
                System.out.println(stock);

                count++;
                totalInserted++;
            }

            if(count == 10) {

                try {
                    System.out.println("Sleeping for 2 seconds...");
                    TimeUnit.SECONDS.sleep(2);

                } catch (InterruptedException ie) {
                    System.err.println("Exception Sleeping: " + ie);
                    Thread.currentThread().interrupt();
                }

            } // end count check

        } // end of while tru

    }

    /**
     *
     * @param args
     */
    public static void main(String[] args) {
        System.out.println("StockTicker starting.");
        new StockTicker();
    }
}

package com.mongodb;

import com.mongodb.Block;
import com.mongodb.client.*;

import com.mongodb.client.MongoClient;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import io.github.cdimascio.dotenv.Dotenv;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.function.Consumer;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import static java.util.Collections.singletonList;
import static java.util.Arrays.asList;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;

/**
 *
 */
public class ChangeStreamListener {

    // MongoDB Variables
    private String mongodbUri;
    private String mongodbDatabaseName;
    private String mongodbCollection;
    private MongoDatabase mongodbDatabase;

    /**
     *
     */
    public ChangeStreamListener() {
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

        // Comment/uncomment whichever one you need to test
        //this.listen();
        this.resumeListen();
    }

    /**
     *
     */
    private void listen() {
        System.out.println("Start listening for stock inserts with price >= $50.00");

        MongoCollection<Stock> stocks = this.mongodbDatabase.getCollection(this.mongodbCollection, Stock.class);

        // Create $match pipeline stage.
        List<Bson> pipeline = singletonList(Aggregates.match(Filters.and(
                Document.parse("{'fullDocument.price': {'$gte': 50.00}}"),
                Filters.in("operationType", asList("insert")))));

        stocks.watch(pipeline).forEach(printEvent());
    }

    /**
     *  Pay attention to the sys out.  the resume token will indicate to start AFTER the change stream
     *  restart.  When reviewing the logs, 3 stocks should print before grabbing a resume token.  When
     *  the new change stream resumes, it will print out the 2nd and 3rd stocks again after the restart.
     */
    private void resumeListen() {
        System.out.println("Resume Token Example");
        System.out.println("Insert Stocks with Price >= $75.00");

        MongoCollection<Stock> stocks = this.mongodbDatabase.getCollection(this.mongodbCollection, Stock.class);

        // Create $match pipeline stage.
        List<Bson> pipeline = singletonList(Aggregates.match(Filters.and(
                Document.parse("{'fullDocument.price': {'$gte': 75.00}}"),
                Filters.in("operationType", asList("insert")))));

        ChangeStreamIterable<Stock> changeStream = stocks.watch(pipeline);
        MongoChangeStreamCursor<ChangeStreamDocument<Stock>> cursor = changeStream.cursor();

        // arbitrary values
        int indexOfOperation = 3;
        int indexWhenSomethingHappened = 6;
        int counter = 0;

        BsonDocument resumeToken = null;

        while(cursor.hasNext() && counter != indexWhenSomethingHappened) {
            ChangeStreamDocument<Stock> event = cursor.next();
            if(indexOfOperation == counter) {
                resumeToken = event.getResumeToken();
                System.out.println("_id of stock for resume: " + event.getFullDocument().getId());
                System.out.println("stockTicker of stock for resume: " + event.getFullDocument().getStockTicker());
                System.out.println("price of stock for resume: " + event.getFullDocument().getPrice());
            }
            System.out.println("Event: " + event);
            counter++;
        }

        System.out.println("===== Something bad occured....need to restart my change stream with my resume token!");
        System.out.println("=========> My resume token: " + resumeToken);

        assert resumeToken != null;
        stocks.watch(pipeline).resumeAfter(resumeToken).forEach(printEvent());
    }

    /**
     *
     * @return
     */
    private static Consumer<ChangeStreamDocument<Stock>> printEvent() {
        return System.out::println;
    }

    /**
     *
     * @param args
     */
    public static void main(String[] args) {
        System.out.println("Starting Change Stream Listener.");
        new ChangeStreamListener();
    }
}

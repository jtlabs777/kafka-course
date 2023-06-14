package org.example.demos;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am a Kafka Consumer");
        var groupId = "my-java-application";
        String topic = "demo_java";

        //create Producer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.27.44.161:9092"); //connect to localhost
        //properties.setProperty("key.serializer", StringSerializer.class.getName());
        //properties.setProperty("value.serializer", StringSerializer.class.getName());

       //create consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);

        properties.setProperty("auto.offset.reset", "earliest"); //cant be none/earliest/latest  , latest is current topics

        //create a consumer
        KafkaConsumer<String, String > consumer = new KafkaConsumer<>(properties);

        //get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        //adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup(); //will trigger an exception when consumer.poll is called.

                // join the main thread to allow the execution of hte code in the main thread
                try {
                    mainThread.join(); //the shutdown thread joins up with main to finish execution of code
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try{
            //subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));

            //poll for data
            while (true) {


                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for(ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shut down");
        } catch (Exception e) {
            log.error("Unexpected exception in the consumer", e);
        } finally {
            consumer.close(); //close the consumer, this will also commit the offsets
            log.info("The consumer is now gracefully shutdown");
        }

    }


}

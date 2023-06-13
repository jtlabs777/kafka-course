package org.example.demos;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am a Kafka Producer");

        //create Producer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.27.44.161:9092"); //connect to localhost
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch_size", "400"); //just for DEMO, dont use this for batch size

        //properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName()); //not recommended

        //create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int j=0; j<2; j++) {
            for (int i = 0; i < 10; i++) {
                String topic = "demo_java";
                String key = "id_" + i;
                String value = "hello world" + i;
                //create a Producer Record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key,  value);

                // send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        //executed every time a record is successfully sent or an exception is thrown
                        if (e == null) {
                            //the record was successfully sent
                            log.info("Key: " + key  + " | Partition: " + metadata.partition());
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }







        //flush and close the producer
            //tell the producer to send all data and block until done -- synchronous
            producer.flush();

            //close the producer, note this automatically calls flush but flush is included to demonstrate availability as API
            producer.close();
    }


}

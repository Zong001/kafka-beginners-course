package io.conductor.demo.kafka;

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
        log.info("I am a  ka Consumer");

        String groupId = "my-java-application";
        String topic = "demo_java";
        //create Prod properties

        Properties properties = new Properties();

        //local host
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //playground
//        properties.setProperty("security.protocal","SASL_SSL");
//        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
//        properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        //consumer config
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id",groupId);
        properties.setProperty("auto.offset.reset","earliest");

        //create a consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        //get a reference to the main thread

        final Thread mainThread = Thread.currentThread();

        // add a shutdown hook

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("detected a shutdown,let us exit by calling consumer.wakeup()>>>");
                consumer.wakeup();

                // join the main thread to allow the exe of the code in the main
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
//                super.run();
            }
        });

        try {


            //sub to a topic

            consumer.subscribe(Arrays.asList(topic));

            // poll for data

            while (true) {
                log.info("polling");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("key: " + record.key() + ", value: " + record.value() + ", Partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is start to shut down");
        } catch (Exception e) {
            log.info("Unexpected exception in the consumer",e);
        } finally {
            consumer.close(); // close the consumer , also commit offsets
            log.info("the consumer is now gracefully shut down");
        }
    }
    // kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic demo_java  --from-beginning
}

package io.conductor.demo.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a  ka prod");

        //create Prod properties

        Properties properties = new Properties();

        //local host
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        //playground
//        properties.setProperty("security.protocal","SASL_SSL");
//        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
//        properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        //set prod prop
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());



        //create Prod

        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);


        for (int i = 0; i < 10;i++){


        //create Prod Record

        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("demo_java","hello world " + i);

        //send data

        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                //exe every time a record sent successfully or an exception thrown
                if (e == null) {
                    //success
                    log.info("Received new metadata \n" +
                            "Topic " + recordMetadata.topic() + "\n" +
                            "Partition " + recordMetadata.partition() + "\n" +
                            "Offset " + recordMetadata.offset() + "\n" +
                            "TimeStamp " + recordMetadata.timestamp() + "\n"
                    );
                }else {
                    log.error("error wile producing" ,e);
                }
            }
        });

        }


        // tell the prod to send all data and block until done -- synchronous

        producer.flush();

        //flush and close Prod

        producer.close();
    }

    //
    //
    //
    // kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic demo_java  --from-beginning
}

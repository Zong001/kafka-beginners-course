package io.conduktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";

//create Prod properties

        Properties properties = new Properties();

        //local host
        properties.setProperty("bootstrap.servers",bootstrapServers);

        //playground
//        properties.setProperty("security.protocal","SASL_SSL");
//        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
//        properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        //set prod prop
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());


        //create Prod

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "wikimedia.recentchange";

        BackgroundEventHandler backgroundEventHandler = new WikimediaChangeHandler(topic,producer);

        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        BackgroundEventSource.Builder builder =  new BackgroundEventSource.Builder(backgroundEventHandler,new EventSource.Builder(URI.create(url)));
        BackgroundEventSource eventSource =  builder.build();

        // start the producer in another thread

        eventSource.start();

        // prod 10 min

        TimeUnit.MINUTES.sleep(10);
    }
}

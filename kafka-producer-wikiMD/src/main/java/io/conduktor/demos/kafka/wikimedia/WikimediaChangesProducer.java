package io.conduktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    public static void main(String[] args) throws InterruptedException{

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


        // set safe prod configs (kafka <= 2.8)

        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");// same as -1
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));// same as -1


        //create Prod

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "wikimedia.recentchange";

        EventHandler eventHandler = new WikimediaChangeHandler(topic,producer);

        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        // 创建带 UA 的 OkHttp client
        OkHttpClient client = new OkHttpClient.Builder()
                .addInterceptor(chain -> {
                    Request req = chain.request().newBuilder()
                            .header("User-Agent",
                                    "KafkaWikimediaDemo/1.0 (your_email@example.com)")
                            .header("Accept", "text/event-stream")
                            .build();
                    return chain.proceed(req);
                })
                .build();

// 传 client 进去
        EventSource.Builder builder =
                new EventSource.Builder(eventHandler, URI.create(url))
                        .client(client);
        EventSource eventSource = builder.build();


        // start the producer in another thread

        eventSource.start();

        // prod 10 min

        TimeUnit.SECONDS.sleep(5);
    }
}

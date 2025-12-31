package io.conduktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.apache.kafka.clients.producer.KafkaProducer;
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


        //create Prod

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "wikimedia.recentchange";

        BackgroundEventHandler backgroundEventHandler = new WikimediaChangeHandler(topic,producer);

        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
//        BackgroundEventSource.Builder builder =  new BackgroundEventSource.Builder(backgroundEventHandler,new EventSource.Builder(URI.create(url)));

//        EventSource.Builder esBuilder =
//                new EventSource.Builder(URI.create(url))
//                        .header("User-Agent", "KafkaWikimediaProducer/1.0 (your_email@example.com)");
//
//        BackgroundEventSource eventSource =
//                new BackgroundEventSource.Builder(handler, esBuilder)
//                        .build();
//
//        BackgroundEventSource eventSource =  builder.build();

//        EventSource.Builder esBuilder =
//                new EventSource.Builder(URI.create(url))
//                        .requestTransformer(request ->
//                                request.newBuilder()
//                                        .addHeader("User-Agent",
//                                                "KafkaWikimediaProducer/1.0 (your_email@example.com)")
//                                        .build()
//                        );
//
//        BackgroundEventSource eventSource =
//                new BackgroundEventSource.Builder(
//                        new WikimediaChangeHandler(topic, producer),
//                        esBuilder
//                ).build();
//        OkHttpClient client = new OkHttpClient.Builder()
//                .addInterceptor(chain -> {
//                    Request original = chain.request();
//                    Request request = original.newBuilder()
//                            .header("User-Agent",
//                                    "KafkaWikimediaProducer/1.0 (your_email@example.com)")
//                            .build();
//                    return chain.proceed(request);
//                })
//                .build();
//
//        EventSource.Builder esBuilder =
//                new EventSource.Builder(URI.create(url))
//                        .client(client);
//
//        BackgroundEventHandler handler =
//                new WikimediaChangeHandler("wikimedia.recentchange", producer);
//
//        BackgroundEventSource eventSource =
//                new BackgroundEventSource.Builder(handler, esBuilder).build();

//        OkHttpClient client = new OkHttpClient.Builder()
//                .addInterceptor(chain -> {
//                    Request original = chain.request();
//                    Request request = original.newBuilder()
//                            .header("User-Agent",
//                                    "KafkaWikimediaProducer/1.0 (your_email@example.com)")
//                            .build();
//                    return chain.proceed(request);
//                })
//                .build();
//
//// 👉 关键区别在这里 —— client 作为构造参数传入
//        EventSource.Builder esBuilder =
//                new EventSource.Builder(URI.create(url));
//
//        BackgroundEventSource eventSource =
//                new BackgroundEventSource.Builder(
//                        new WikimediaChangeHandler(topic, producer),
//                        esBuilder
//                ).build();

        OkHttpClient client = new OkHttpClient.Builder()
                .addInterceptor(chain -> {
                    Request request = chain.request().newBuilder()
                            .header("User-Agent",
                                    "KafkaWikimediaDemo/1.0 (https://github.com/yourname; your_email@example.com)")
                            .header("Accept", "text/event-stream")
                            .build();
                    return chain.proceed(request);
                })
                .build();

// 注意：client 作为构造参数传入
        EventSource.Builder esBuilder =
                new EventSource.Builder(URI.create(url), client);

        BackgroundEventSource eventSource =
                new BackgroundEventSource.Builder(
                        new WikimediaChangeHandler(topic, producer),
                        esBuilder
                ).build();


        // start the producer in another thread

        eventSource.start();

        // prod 10 min

        TimeUnit.MINUTES.sleep(10);
    }
}

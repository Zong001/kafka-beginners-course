package io.conduktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WikimediaChangeHandler implements BackgroundEventHandler {


    KafkaProducer<String, String> producer ;

    String topic ;

    private final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getName());

    public WikimediaChangeHandler(String topic, KafkaProducer<String, String> producer) {
        this.topic = topic;
        this.producer = producer;
    }

    @Override
    public void onOpen() throws Exception {

        //nothing here
    }

    @Override
    public void onClosed() throws Exception {
        producer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {

        log.info(messageEvent.getData());
        // async

        producer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String s) throws Exception {

        //nothing
    }

    @Override
    public void onError(Throwable throwable) {

        log.error("Error in Stream Reading",throwable);
    }
}

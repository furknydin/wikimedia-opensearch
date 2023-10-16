package org.demo.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String TOPIC = "wikimedia.recentchange";
    private static final String WIKIMEDIA_URL = "https://stream.wikimedia.org/v2/stream/recentchange";
    public static void main(String[] args) throws InterruptedException {
        KafkaProducer<String, String> producer = getKafkaProducer();

        EventHandler eventHandler = new WikimediaChangeHandler(producer,TOPIC);

        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(WIKIMEDIA_URL));
        EventSource eventSource = builder.build();

        //Start producer in another thread
        eventSource.start();

        // we produce for 10 minutes and block the program until
        TimeUnit.MINUTES.sleep(10);

    }
    @NotNull
    private static KafkaProducer<String, String> getKafkaProducer() {
        Properties properties = new Properties();

        //create producer properties
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //set safe producer configs (Kafka <= 2.8, and there is default value for 2.8 and upper)
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));

        //set high throughput producer configs
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        //create the producer
        return new KafkaProducer<>(properties);
    }
}
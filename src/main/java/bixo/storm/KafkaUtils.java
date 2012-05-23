package bixo.storm;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

public class KafkaUtils {

    public static Producer<String, UrlDatum> createUrlProducer() {
        Properties producerProps = new Properties();
        producerProps.put("broker.list", "0:localhost:9092");
        producerProps.put("serializer.class", "bixo.storm.UrlDatumEncoder");
        ProducerConfig config = new ProducerConfig(producerProps);
        return new Producer<String, UrlDatum>(config);
    }
}

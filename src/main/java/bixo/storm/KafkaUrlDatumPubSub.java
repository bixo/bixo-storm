package bixo.storm;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaMessageStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;

public class KafkaUrlDatumPubSub implements IUrlDatumPubSub {

    private String _topic;
    private Producer<String, UrlDatum> _producer;
    
    public KafkaUrlDatumPubSub(String topic) {
        _topic = topic;
        
        Properties producerProps = new Properties();
        producerProps.put("broker.list", "0:localhost:9092");
        producerProps.put("serializer.class", "bixo.storm.UrlDatumEncoder");
        ProducerConfig config = new ProducerConfig(producerProps);
        _producer = new Producer<String, UrlDatum>(config);
    }
    
    @Override
    public Iterator<UrlDatum> iterator() {
        
        Properties consumerProps = new Properties();
        consumerProps.put("groupid", CrawlerConfig.KAFKA_GROUP_ID);
        
        // TODO - figure out what's really needed here.
        // consumerProps.put("consumer.timeout.ms", "" + MAX_CONSUMER_DURATION);
        consumerProps.put("zk.connect", "127.0.0.1:2181");
        consumerProps.put("zk.connectiontimeout.ms", "1000000");
        consumerProps.put("broker.list", "0:localhost:9092");

        // Create the connection to the cluster
        ConsumerConfig consumerConfig = new ConsumerConfig(consumerProps);
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

        // We create a single consumer per call to get an interator.
        Map<String, Integer> streamInfo = new HashMap<String, Integer>();
        streamInfo.put(_topic, 1);

        KafkaMessageStream<UrlDatum> messages = consumerConnector.createMessageStreams(streamInfo, 
                        new UrlDatumDecoder()).get(_topic).get(0);
        
        return messages.iterator();
    }

    @Override
    public void publish(UrlDatum url) {
        ProducerData<String, UrlDatum> data = new ProducerData<String, UrlDatum>(_topic, url);
        _producer.send(data);
    }

}

package bixo.storm;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaMessageStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.message.Message;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringDecoder;

import org.apache.log4j.Logger;
import org.apache.tools.ant.taskdefs.Sleep;

import bixo.utils.ThreadedExecutor;

public class CrawlDB implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(CrawlDB.class);
    
    private Map<Long, UrlDatum> _urls;
    private volatile boolean _active;
    
    public CrawlDB(UrlDatum... seedUrls) {
        _urls = new HashMap<Long, UrlDatum>();
        for (UrlDatum url : seedUrls) {
            _urls.put(StringUtils.getLongHash(url.getUrl()), url);
        }
        
    }
    
    @Override
    public void run() {
        _active = true;
        
        // Set up to be a Kafka producer, producing URLs to be fetched
        Producer<String, UrlDatum> producer = KafkaUtils.createUrlProducer();
        
        // Set up to be a Kafka consumer, handling URL status updates, and new URLs.
        Properties consumerProps = new Properties();
        consumerProps.put("groupid", CrawlerConfig.KAFKA_GROUP_ID);
        
        // TODO - figure out what's really needed here.
        consumerProps.put("consumer.timeout.ms", "" + CrawlerConfig.MAX_CONSUMER_DURATION);
        consumerProps.put("zk.connect", "127.0.0.1:2181");
        consumerProps.put("zk.connectiontimeout.ms", "1000000");
        consumerProps.put("broker.list", "0:localhost:9092");

        // Create the connection to the cluster
        ConsumerConfig consumerConfig = new ConsumerConfig(consumerProps);
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
        
        // create two threads
        ThreadedExecutor executor = new ThreadedExecutor(2, CrawlerConfig.MAX_CONSUMER_DURATION);
        Map<String, Integer> streamInfo = new HashMap<String, Integer>();
        streamInfo.put(CrawlerConfig.KAFKA_UPDATE_TOPIC, 2);
        
        List<KafkaMessageStream<Message>> streams = consumerConnector.createMessageStreams(streamInfo).get(CrawlerConfig.KAFKA_UPDATE_TOPIC);
        for (final KafkaMessageStream<Message> stream : streams) {
            LOGGER.info("Executing runnable for stream " + stream.topic());
            
            executor.execute(new Runnable() {
                public void run() {
                    UrlDatumDecoder decoder = new UrlDatumDecoder();
                    
                    ConsumerIterator<Message> iter = stream.iterator();
                    while (iter.hasNext()) {
                        Message message = iter.next();
                        UrlDatum newUrl = decoder.toEvent(message);
                        LOGGER.info("URL to consume: " + newUrl);
                        long urlhash = StringUtils.getLongHash(newUrl.getUrl());

                        synchronized (_urls) {
                            UrlDatum curUrl = _urls.get(urlhash);

                            if (curUrl == null) {
                                // No current entry, better be a new.
                                if (!newUrl.getStatus().equals("new")) {
                                    // Houston, we have a problem.
                                    LOGGER.error("Url status being updated, but doesn't exist: " + newUrl);
                                } else {
                                    _urls.put(urlhash, newUrl);
                                }
                            } else {
                                // We need to merge this into a current entry. We always ignore new URLs,
                                // as anything in the crawlDB is more important.
                                if (!newUrl.getStatus().equals("new")) {
                                    // TODO - do real merge.
                                    _urls.put(urlhash, newUrl);
                                }
                            }
                        }
                    } 
                }
            });
        }

        try {
            // We want to continuously generate URLs to be fetched
            while (!Thread.interrupted()) {
                try {
                    Thread.sleep(100);

                    synchronized (_urls) {
                        // Find a URL to fetch
                        for (Entry<Long, UrlDatum> url : _urls.entrySet()) {
                            if (url.getValue().getStatus() == "unfetched") {
                                LOGGER.info("Publishing URL to Kafka: " + url.getValue().getUrl());
                                
                                ProducerData<String, UrlDatum> data = new ProducerData<String, UrlDatum>(CrawlerConfig.KAFKA_FETCH_TOPIC, url.getValue());
                                producer.send(data);

                                url.getValue().setStatus("fetching");
                                break;
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        } finally {
            LOGGER.info("Closing the CrawlDB Kafka producer");
            producer.close();

            LOGGER.info("Shutting down CrawlDB Kafka consumer");
            consumerConnector.shutdown();
            
            // Now we need to shut down the executor, since we're doing running.
            LOGGER.info("Terminating CrawlDB Kafka consumer threads");

            try {
                // TODO set appropriate termination wait time
                executor.terminate(10000);
            } catch (InterruptedException e) {
                // TODO what to do here?
            }

            _active = false;
        }
    }
    
    public boolean isActive() {
        return _active;
    }
    
    public void terminate(Thread t) {
        t.interrupt();
        
        // TODO - what about timeout here?
        while (isActive() && !Thread.interrupted()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // TODO - what to do here?
                Thread.currentThread().interrupt();
            }
        }
    }
}

package bixo.storm;

import java.nio.channels.ClosedByInterruptException;
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
    private KafkaTopic _urlPublisher;
    private KafkaTopic _urlOutlinks;
    private KafkaTopic _urlUpdates;
    private volatile boolean _active;

    public CrawlDB(KafkaTopics pubSub, UrlDatum... seedUrls) {
        _urlPublisher = pubSub.getTopic(KafkaTopics.FETCH_URLS_TOPIC_NAME);
        _urlOutlinks = pubSub.getTopic(KafkaTopics.OUTLINK_URLS_TOPIC_NAME);
        _urlUpdates = pubSub.getTopic(KafkaTopics.UPDATE_URLS_TOPIC_NAME);

        _urls = new HashMap<Long, UrlDatum>();
        for (UrlDatum url : seedUrls) {
            _urls.put(StringUtils.getLongHash(url.getUrl()), url);
        }

    }

    @Override
    public void run() {
        _active = true;

        // create two threads, one for getting updated URLs, and the other for
        // getting outlinks.
        ThreadedExecutor executor = new ThreadedExecutor(2, KafkaTopics.MAX_CONSUMER_DURATION);

        executor.execute(new Runnable() {
            public void run() {
                for (UrlDatum url : _urlUpdates) {
                    LOGGER.info("Consumed updated URL from Kafka: " + url);
                    long urlhash = StringUtils.getLongHash(url.getUrl());

                    synchronized (_urls) {
                        UrlDatum curUrl = _urls.get(urlhash);

                        if (curUrl == null) {
                            LOGGER.error("Url status being updated, but doesn't exist: " + url);
                        } else {
                            _urls.put(urlhash, url);
                        }
                    }
                }
            }
        });

        executor.execute(new Runnable() {
            public void run() {
                for (UrlDatum url : _urlOutlinks) {
                    LOGGER.info("Consumed outlink URL from Kafka: " + url);
                    long urlhash = StringUtils.getLongHash(url.getUrl());

                    synchronized (_urls) {
                        UrlDatum curUrl = _urls.get(urlhash);

                        if (curUrl == null) {
                            _urls.put(urlhash, url);
                        }
                    }
                }
            }
        });

        // We also need to continuously generate URLs to be fetched.
        try {
            while (!Thread.interrupted()) {
                try {
                    Thread.sleep(100);

                    synchronized (_urls) {
                        // Find a URL to fetch
                        for (Entry<Long, UrlDatum> url : _urls.entrySet()) {
                            if (url.getValue().getStatus() == "unfetched") {
                                LOGGER.info("Publishing URL to Kafka: " + url.getValue().getUrl());

                                _urlPublisher.publish(url.getValue());

                                url.getValue().setStatus("fetching");
                                break;
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    LOGGER.info("CrawlDB interrupted");
                    Thread.currentThread().interrupt();
                }
            }
        } finally {
            // Now we need to shut down the executor, since we're doing running.
            LOGGER.info("Terminating CrawlDB Kafka consumer threads");

            try {
                // TODO set appropriate termination wait time
                if (!executor.terminate(100L)) {
                    // TODO - This always happens, but we'd like to avoid it
                    LOGGER.warn("Couldn't terminate CrawlDB Kakfa consumers");
                }
            } catch (InterruptedException e) {
                // TODO what to do here?
                LOGGER.error("Interrupted while terminating CrawlDB Kakfa consumers");
            }

            // We need to explicitly close down our publisher.
            _urlPublisher.close();
            
            _active = false;
        }
    }

    public boolean isActive() {
        return _active;
    }

}

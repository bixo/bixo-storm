package bixo.storm;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Properties;

import org.apache.log4j.Logger;

import kafka.api.FetchRequest;
import kafka.consumer.SimpleConsumer;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import kafka.producer.ProducerConfig;

@SuppressWarnings("serial")
public class KafkaTopic implements Iterable<UrlDatum>, Serializable {
    private static final Logger LOGGER = Logger.getLogger(KafkaTopic.class);
    
    private String _topicName;

    private transient Producer<String, UrlDatum> _producer;
    private transient SimpleConsumer _consumer;
    
    public KafkaTopic(String topicName) {
        _topicName = topicName;
    }
    
    public String getTopicName() {
        return _topicName;
    }

    public Iterator<UrlDatum> iterator() {
        // TODO support real vs. test settings for the SimpleConsumer
        _consumer = new SimpleConsumer("localhost", 9090, 100, 1024);
        
        return new Iterator<UrlDatum>() {

            private long offset = 0;
            private scala.collection.Iterator<MessageAndOffset> msgs = null;
            private UrlDatumDecoder decoder = new UrlDatumDecoder();
            
            @Override
            public boolean hasNext() {
                // TODO switch to pulling msgs immediately and loading into a list?
                
                while ((msgs == null) || !msgs.hasNext()) {
                    // TODO the code below doesn't solve our issue with interrupts not working,
                    // so we need to do something else.
                    // Use -1 for time to indicate we want any offsets from the most recent.
                    try {
                        long[] offsets = _consumer.getOffsetsBefore(getTopicName(), 0, -1L, 1);
                        if (offsets[0] == offset) {
                            // No offsets available, so sleep and loop.
                            // LOGGER.info(String.format("%s: no messages, sleeping", getTopicName()));
                            Thread.sleep(1000L);
                            continue;
                        }
                    } catch (InterruptedException e) {
                        // LOGGER.info(String.format("%s: interrupted in hasNext while waiting for messages", getTopicName()));
                        return false;
                    } catch (Exception e) {
                        // We can get a java.nio.channels.ClosedByInterruptException, but Java apparently
                        // doesn't know that this can be thrown by Scala code.
                        // LOGGER.info(String.format("%s: interrupted in hasNext while checking for messages", getTopicName()));
                        return false;
                    }

                    
                    try {
                        // LOGGER.info(String.format("%s: getting messages", getTopicName()));
                        FetchRequest fr = new FetchRequest(getTopicName(), 0, offset, 1024);
                        // TODO - currently this blocks even when we're interrupted, since
                        // SimpleConsumer swallows the interrupt and just reconnects.
                        ByteBufferMessageSet msgSet = _consumer.fetch(fr);
                        msgs = msgSet.iterator();
                        // LOGGER.info(String.format("%s: got messages", getTopicName()));
                    } catch (Exception e) {
                        // We can get a java.nio.channels.ClosedByInterruptException???
                        // Seems like this would only happen if the fetch() call above
                        // got two interrupts in a row.
                        return false;
                    }
                }
                
                return true;
            }

            @Override
            public UrlDatum next() {
                if (!hasNext()) {
                    throw new IllegalStateException("next called when hasNext returned false");
                }
                
                MessageAndOffset msg = msgs.next();
                offset = msg.offset();
                
                return decoder.toEvent(msg.message());
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public synchronized void publish(UrlDatum url) {
        // TODO - need to synchronize so we don't get two producers here
        if (_producer == null) {
            LOGGER.info(String.format("%s: creating producer", getTopicName()));
            
            Properties producerProps = new Properties();
            producerProps.setProperty("producer.type", "async");
            producerProps.setProperty("queue.time", "2000");
            producerProps.setProperty("queue.size", "100");
            producerProps.setProperty("batch.size", "10");
            producerProps.setProperty("broker.list", "1:localhost:9090");
            producerProps.put("serializer.class", "bixo.storm.UrlDatumEncoder");
            ProducerConfig config = new ProducerConfig(producerProps);
            _producer = new Producer<String, UrlDatum>(config);
        }
        
        LOGGER.info(String.format("%s: publishing %s", getTopicName(), url.getUrl()));
        ProducerData<String, UrlDatum> data = new ProducerData<String, UrlDatum>(getTopicName(), url);
        _producer.send(data);
    }
    
    public synchronized void close() {
        if (_producer != null) {
            LOGGER.info(String.format("%s: closing producer", getTopicName()));
            _producer.close();
            _producer = null;
        }
    }
    
    @Override
    public String toString() {
        return String.format("Kafka topic %s", getTopicName());
    }

}

package bixo.storm;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

@SuppressWarnings("serial")
public class LocalPubSubTopic extends BasePubSubTopic {
    private static final Logger LOGGER = Logger.getLogger(LocalPubSubTopic.class);
    
    private List<UrlDatum> _queue;
    
    public LocalPubSubTopic(String topic) {
        super(topic);
        
        _queue = Collections.synchronizedList(new LinkedList<UrlDatum>());
    }
    
    public boolean isEmpty() {
        return _queue.size() == 0;
    }
    
    @Override
    public Iterator<UrlDatum> iterator() {
        return new Iterator<UrlDatum>() {

            @Override
            public boolean hasNext() {
                while (isEmpty() && !Thread.interrupted()) {
                    try {
                        LOGGER.debug(String.format("Nothing in topic %s, sleeping", getTopicName()));
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        return false;
                    }
                }
                
                return !isEmpty();
            }

            @Override
            public UrlDatum next() {
                UrlDatum url = _queue.remove(0);
                LOGGER.debug(String.format("Consuming from topic %s: %s", getTopicName(), url));
                return url;
            }

            @Override
            public void remove() {
                _queue.remove(0);
            }
        };
    }

    @Override
    public void publish(UrlDatum url) {
        LOGGER.debug(String.format("Publishing to topic %s: %s", getTopicName(), url));
        
        _queue.add(url);
    }

}

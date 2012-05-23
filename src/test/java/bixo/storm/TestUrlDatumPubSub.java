package bixo.storm;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

public class TestUrlDatumPubSub implements IUrlDatumPubSub {
    private static final Logger LOGGER = Logger.getLogger(TestUrlDatumPubSub.class);
    
    private String _topic;
    private List<UrlDatum> _queue;
    
    public TestUrlDatumPubSub(String topic) {
        _topic = topic;
        _queue = Collections.synchronizedList(new LinkedList<UrlDatum>());
    }
    
    @Override
    public Iterator<UrlDatum> iterator() {
        return new Iterator<UrlDatum>() {

            @Override
            public boolean hasNext() {
                while ((_queue.size() == 0) && !Thread.interrupted()) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        return false;
                    }
                }
                
                return true;
            }

            @Override
            public UrlDatum next() {
                UrlDatum url = _queue.remove(0);
                LOGGER.debug(String.format("Consuming from topic %s: %s", _topic, url));
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
        LOGGER.debug(String.format("Publishing to topic %s: %s", _topic, url));
        
        _queue.add(url);
    }

}

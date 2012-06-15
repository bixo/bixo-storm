package bixo.storm;

import static org.junit.Assert.*;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import backtype.storm.utils.Utils;

public class LocalPubSubTopicTest {

    @Test
    public void simpleTest() {
        LocalPubSubTopic topic = new LocalPubSubTopic("topic");
        topic.publish(new UrlDatum("url", "status"));
        
        assertTrue(topic.iterator().hasNext());
        assertEquals(new UrlDatum("url", "status"), topic.iterator().next());
        Thread.currentThread().interrupt();
        assertFalse(topic.iterator().hasNext());
    }
    
    @Test
    public void multiThreadTest() throws Exception {
        final LocalPubSubTopic topic = new LocalPubSubTopic("topic");

        Runnable publisher = new Runnable() {
            
            @Override
            public void run() {
                for (int i = 0; i < 5; i++) {
                    topic.publish(new UrlDatum("http://domain.com/page" + i, "unfetched"));
                    Utils.sleep(50);
                }
            }
        };
        
        final AtomicInteger numUrls = new AtomicInteger(0);

        Runnable subscriber = new Runnable() {
            
            @Override
            public void run() {
                for (UrlDatum url : topic) {
                    numUrls.incrementAndGet();
                }
            }
        };
        
        Thread pubThread = new Thread(publisher);
        Thread subThread = new Thread(subscriber);
        
        pubThread.start();
        subThread.start();
        
        while (pubThread.isAlive()) {
            // We have to sleep significantly longer than the sleep time in the
            // PubSub iterator.
            Utils.sleep(200);
        }
        
        // Now we should be able to interrupt the subscriber thread, which
        // will cause it to terminate.
        
        subThread.interrupt();
        
        assertEquals(5, numUrls.get());
    }

}

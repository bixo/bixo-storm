package bixo.storm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.log4j.Logger;
import org.junit.Test;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;

public class CrawlerTopologyTest {
    private static final Logger LOGGER = Logger.getLogger(CrawlerTopologyTest.class);
    
    @Test
    public void test() {
        LocalPubSub topics = new LocalPubSub();
        
        LocalPubSubTopic urlsToFetch = new LocalPubSubTopic(IPubSub.FETCH_URLS_TOPIC_NAME);
        LOGGER.info("Created publisher " + urlsToFetch);
        topics.addTopic(urlsToFetch);
        
        LocalPubSubTopic urlsToUpdate = new LocalPubSubTopic(IPubSub.UPDATE_URLS_TOPIC_NAME);
        topics.addTopic(urlsToUpdate);
        
        LocalPubSubTopic outlinkUrls = new LocalPubSubTopic(IPubSub.OUTLINK_URLS_TOPIC_NAME);
        topics.addTopic(outlinkUrls);
        
        StormTopology topology = CrawlerTopology.createTopology(topics);
        Config stormConf = new Config();
        stormConf.setDebug(true);
        stormConf.setMaxTaskParallelism(3);

        LocalCluster stormCluster = new LocalCluster();
        stormCluster.submitTopology("bixo-storm", stormConf, topology);
        
        // Publish a URL
        urlsToFetch.publish(new UrlDatum("http://cnn.com", "unfetched"));

        Utils.sleep(5000);
        
        // Make sure we don't have any URLs to fetch.
        assertTrue(urlsToFetch.isEmpty());

        // Make sure we have a fetched URL in our results
        // assertFalse(urlsToUpdate.isEmpty());
        // assertEquals(new UrlDatum("http://cnn.com", "fetched"), urlsToUpdate.iterator().next());
        // assertTrue(urlsToUpdate.isEmpty());
        
        // We should have two URLs in the outlinks.
//        assertFalse(outlinkUrls.isEmpty());
//        assertEquals(new UrlDatum("http://cnn.com/page1", "unfetched"), outlinkUrls.iterator().next());
//        assertFalse(outlinkUrls.isEmpty());
//        assertEquals(new UrlDatum("http://cnn.com/page2", "unfetched"), outlinkUrls.iterator().next());
//        assertTrue(outlinkUrls.isEmpty());
    }

}

package bixo.storm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;

public class CrawlerTopologyTest {
    private static final Logger LOGGER = Logger.getLogger(CrawlerTopologyTest.class);
    
    private static final String DATA_DIR = "build/test/CrawlerTopologyTest/kafka/";
    
    private KafkaServer _kafkaServer;
    private Thread _crawlDbThread;
    
    @Before
    public void setUp() throws IOException {
        Properties props = new Properties();
        props.setProperty("hostname", "localhost");
        // TODO - use real constants here
        props.setProperty("port", "9090");
        props.setProperty("brokerid", "1");
        
        props.setProperty("log.dir", DATA_DIR);
        FileUtils.deleteDirectory(new File(DATA_DIR));
        
        props.setProperty("enable.zookeeper", "false");
        
        _kafkaServer = new KafkaServer(new KafkaConfig(props));
        _kafkaServer.startup();
        
    }
    
    @After
    public void tearDown() {
        // TODO put this code into static CrawlDB method, and then call shutdown on it
        // Or better yet, create CrawlDbServer with startup, shutdown, and awaitShutdown calls
        // the same as Kafka
        if (_crawlDbThread != null) {
            LOGGER.info("Interrupting CrawlDB");
            _crawlDbThread.interrupt();
            while (_crawlDbThread.isAlive()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    // TODO what to do here?
                }
            }
            _crawlDbThread = null;
        }

        
        if (_kafkaServer != null) {
            _kafkaServer.shutdown();
            _kafkaServer.awaitShutdown();
            _kafkaServer = null;
        }
    }
    
    @Test
    public void test() {
        KafkaTopics topics = KafkaTopics.getDefaultTopics();
        
        _crawlDbThread = new Thread(new CrawlDB(topics, new UrlDatum("http://cnn.com", "unfetched")));
        _crawlDbThread.start();

        StormTopology topology = CrawlerTopology.createTopology(topics);
        Config stormConf = new Config();
        stormConf.setDebug(true);
        stormConf.setMaxTaskParallelism(3);

        LocalCluster stormCluster = new LocalCluster();
        stormCluster.submitTopology("bixo-storm", stormConf, topology);
        
        Utils.sleep(10000);
        
        LOGGER.info("Shutting down Storm cluster...");
        stormCluster.shutdown();
        LOGGER.info("Storm cluster shut down");

        // Make sure CrawlDB has the expected URLs w/status
        // TODO make it so.
    }

}

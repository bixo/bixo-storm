package bixo.storm;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;

public class CrawlerTool {
    private static final Logger LOGGER = Logger.getLogger(CrawlerTool.class);

    private static KafkaServer makeKafkaServer() throws IOException {
        Properties props = new Properties();
        props.setProperty("hostname", "localhost");
        // TODO - use real constants here
        props.setProperty("port", "9090");
        props.setProperty("brokerid", "1");
        // TODO - use appropriate temp directory location
        props.setProperty("log.dir", "/tmp/embeddedkafka/");
        // TODO - only clear out log dir if we're in test mode.
        FileUtils.deleteDirectory(new File("/tmp/embeddedkafka"));
        
        props.setProperty("enable.zookeeper", "false");
        
        KafkaServer server = new KafkaServer(new KafkaConfig(props));
        server.startup();
        
        return server;
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        KafkaServer kafkakServer = null;
        Thread crawlDbThread = null;
        LocalCluster stormCluster = null;
        KafkaTopics topics = null;
        
        try {
            kafkakServer = makeKafkaServer();
            
            // Now we want to set up our topology. This will continuously
            // process URLs that the spout gets from Kafka, and update the
            // state of the URLs (and add new ones) by sending messages to
            // the crawlDB using Kafka.
            topics = KafkaTopics.getDefaultTopics();
            
            StormTopology topology = CrawlerTopology.createTopology(topics);
            Config stormConf = new Config();
            stormConf.setDebug(true);
            stormConf.setMaxTaskParallelism(3);

            // TODO Storm cluster starting up seems to cause problems with zookeeper?
            // Storm in local mode also creates local zookeeper.

            stormCluster = new LocalCluster();
            stormCluster.submitTopology("bixo-storm", stormConf, topology);
        
            crawlDbThread = new Thread(new CrawlDB(topics, new UrlDatum("http://cnn.com", "unfetched")));
            crawlDbThread.start();
            
            // Wait a bit for things to settle down
            Thread.sleep(10000);
        } catch (Exception e) {
            LOGGER.error("Exception running CrawlerTool: " + e.getMessage(), e);
            System.exit(-1);
        } finally {
            // TODO put this code into static CrawlDB method, and then call shutdown on it
            if ((crawlDbThread != null) && crawlDbThread.isAlive()) {
                LOGGER.info("Interrupting CrawlDB");
                crawlDbThread.interrupt();
                while (crawlDbThread.isAlive()) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        // TODO what to do here?
                    }
                }
                
                crawlDbThread = null;
            }
            
            if (stormCluster != null) {
                LOGGER.info("Shutting down Storm cluster...");
                stormCluster.shutdown();
                LOGGER.info("Storm cluster shut down");
            }
            
            if (kafkakServer != null) {
                kafkakServer.shutdown();
                kafkakServer.awaitShutdown();
            }
            

            
        }
    }

}

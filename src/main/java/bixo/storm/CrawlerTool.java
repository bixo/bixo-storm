package bixo.storm;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;

public class CrawlerTool {
    private static final Logger LOGGER = Logger.getLogger(CrawlerTool.class);

    private static class KafkaRunnable implements Runnable {
        
        public static Thread start() throws IOException, InterruptedException {
            KafkaRunnable kafkaRunnable = new KafkaRunnable();
            
            Thread result = new Thread(kafkaRunnable);
            result.start();
            
            // wait for the isActive call to return true.
            while (!kafkaRunnable.isAlive()) {
                Thread.sleep(100);
            }
            
            return result;
        }


        private KafkaConfig _serverConfig;
        private volatile boolean _alive = false;
        
        public KafkaRunnable() throws IOException {
            LOGGER.info("Creating KafkaRunnable...");

            FileInputStream propStream = new FileInputStream("src/test/resources/kafka.properties");
            Properties props = new Properties();
            props.load(propStream);

            _serverConfig = new KafkaConfig(props);
            
            File kafkaLogDir = new File(_serverConfig.logDir());
            FileUtils.deleteDirectory(kafkaLogDir);
            
            LOGGER.info("Created KafkaRunnable");
        }
        
        @Override
        public void run() {
            LOGGER.info("Starting KafkaRunnable...");
            
            KafkaServerStartable kafkaServerStartable = new KafkaServerStartable(_serverConfig);
            kafkaServerStartable.startup();
            _alive = true;
            
            while (!Thread.interrupted()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    // Get us out of the loop.
                    Thread.currentThread().interrupt();
                }
            }
            
            LOGGER.info("Stopping KafkaRunnable");
            
            kafkaServerStartable.shutdown();
            kafkaServerStartable.awaitShutdown();
            _alive = false;
            
            LOGGER.info("Exiting KafkaRunnable");
        }
        
        public boolean isAlive() {
            return _alive;
        }
    }
    
    private static class ZooKeeperRunnable implements Runnable {

        public static Thread start(ServerConfig config) throws IOException, InterruptedException {
            ZooKeeperRunnable zkRunnable = new ZooKeeperRunnable(config);
            
            Thread result = new Thread(zkRunnable);
            result.start();
            
            // wait for the isActive call to return true.
            while (!zkRunnable.isAlive()) {
                Thread.sleep(100);
            }
            
            return result;
        }

        private ZooKeeperServer zkServer;
        private InetSocketAddress zkPort;
        private int zkMaxConnections;
        private volatile boolean _alive = false;
        private NIOServerCnxn.Factory cnxnFactory = null;
        
        public ZooKeeperRunnable(ServerConfig config) throws IOException {
            LOGGER.info("Creating ZooKeeperRunnable...");
            
            // Clear out data from previous runs
            // TODO is this the right way to do it? Check ZooKeeper tests?
            File dataDir = new File(config.getDataDir());
            FileUtils.deleteDirectory(dataDir);
            
            File dataLogDir = new File(config.getDataLogDir());
            FileUtils.deleteDirectory(dataLogDir);

            
            zkServer = new ZooKeeperServer();
            
            FileTxnSnapLog ftxn = new FileTxnSnapLog(new
                   File(config.getDataLogDir()), new File(config.getDataDir()));
            zkServer.setTxnLogFactory(ftxn);
            zkServer.setTickTime(config.getTickTime());
            zkServer.setMinSessionTimeout(config.getMinSessionTimeout());
            zkServer.setMaxSessionTimeout(config.getMaxSessionTimeout());
            
            zkPort = config.getClientPortAddress();
            zkMaxConnections = config.getMaxClientCnxns();
            
            LOGGER.info("Created ZooKeeperRunnable");
        }
        
        @Override
        public void run() {
            LOGGER.info("Running ZooKeeperRunnable");

            try {
                cnxnFactory = new NIOServerCnxn.Factory(zkPort, zkMaxConnections);

                cnxnFactory.startup(zkServer);
                _alive = true;
                cnxnFactory.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                throw new RuntimeException("Exception starting ZooKeeper", e);
            } finally {
                LOGGER.info("Stopping ZooKeeperRunnable");

                if (zkServer.isRunning()) {
                    zkServer.shutdown();
                }
                
                if (cnxnFactory != null) {
                    cnxnFactory.shutdown();
                }
                
                // We can't set alive to false, as the connection factory
                // and/or the ZooKeeper server might still be shutting down
                // _alive = false;
            }
            
            LOGGER.info("Exiting ZooKeeperRunnable");
        }
        
        public boolean isAlive() {
            boolean connectionAlive = (cnxnFactory != null) && (cnxnFactory.isAlive());
            boolean serverAlive = (zkServer != null) && (zkServer.isRunning());
            return _alive && (connectionAlive || serverAlive);
        }
        
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        Thread zookeeperThread = null;
        Thread kafkaThread = null;
        Thread crawlDbThread = null;
        LocalCluster stormCluster = null;

        try {
            ServerConfig zookeeperConfig = new ServerConfig();
            zookeeperConfig.parse("src/test/resources/zookeeper.properties");
            zookeeperThread = ZooKeeperRunnable.start(zookeeperConfig);
            
            kafkaThread = KafkaRunnable.start();
            
            // Now we want to set up our topology. This will continuously
            // process URLs that the spout gets from Kafka, and update the
            // state of the URLs (and add new ones) by sending messages to
            // the crawlDB using Kafka.
            StormTopology topology = CrawlerTopology.createTopology(new LocalPubSub());
            Config stormConf = new Config();
            stormConf.setDebug(true);
            stormConf.setMaxTaskParallelism(3);

            // TODO Storm cluster starting up seems to cause problems with zookeeper?
            // Storm in local mode also creates local zookeeper.

            stormCluster = new LocalCluster();
            stormCluster.submitTopology("bixo-storm", stormConf, topology);
        
            crawlDbThread = new Thread(new CrawlDB(new UrlDatum("http://cnn.com", "unfetched")));
            crawlDbThread.start();
            
            // Wait a bit for things to settle down
            Thread.sleep(10000);
        } catch (Exception e) {
            LOGGER.error("Exception running CrawlerTool: " + e.getMessage(), e);
            System.exit(-1);
        } finally {
            if (stormCluster != null) {
                LOGGER.info("Shutting down Storm cluster...");
                stormCluster.shutdown();
                LOGGER.info("Storm cluster shut down");
            }
            
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
            
            if ((kafkaThread != null) && kafkaThread.isAlive()) {
                LOGGER.info("Interrupting KafkaRunnable thread...");
                kafkaThread.interrupt();
                
                // TODO - timeout? Leverage support for termination?
                while (kafkaThread.isAlive()) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        // TODO what to do here?
                    }
                }
                
                LOGGER.info("KafkaRunnable thread finished");
                kafkaThread = null;
            }
            
            if ((zookeeperThread != null) && zookeeperThread.isAlive()) {
                LOGGER.info("Interrupting ZooKeeperRunnable...");
                zookeeperThread.interrupt();
                
                // TODO - timeout? Leverage support for termination?
                while (zookeeperThread.isAlive()) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        // TODO what to do here?
                    }
                }
                
                LOGGER.info("ZooKeeperRunnable thread finished");
                zookeeperThread = null;
            }
        }
    }

}

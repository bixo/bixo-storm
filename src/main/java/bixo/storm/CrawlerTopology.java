package bixo.storm;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import bixo.robots.SimpleRobotRules;
import bixo.utils.DomainInfo;

/**
 * A topology for crawling pages.
 */
public class CrawlerTopology {
    private static final Logger LOGGER = Logger.getLogger(CrawlerTopology.class);
    
    @SuppressWarnings("serial")
    public static class AddHostname extends BaseBasicBolt {
        
        private transient Producer<String, UrlDatum> _producer;
        
        @SuppressWarnings("rawtypes")
        @Override
        public void prepare(Map stormConf, TopologyContext context) {
            super.prepare(stormConf, context);
            
            _producer = KafkaUtils.createUrlProducer();
        }
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("url", "status", "hostname"));
        }

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            String url = input.getStringByField("url");
            String status = input.getStringByField("status");
            String hostname = "UNKNOWN";
            
            try {
                URL realUrl = new URL(url);
                hostname = realUrl.getHost();
            } catch (MalformedURLException e) {
                _producer.send(new ProducerData<String, UrlDatum>(CrawlerConfig.KAFKA_UPDATE_TOPIC, new UrlDatum(url, "invalid-url")));
                // TODO ack immediately
            }
            
            collector.emit(new Values(url, status, hostname));
        }
    }  
    
    @SuppressWarnings("serial")
    public static class TupleLogger extends BaseBasicBolt {
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("null"));
        }

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            LOGGER.info("TupleLogger: " + input.toString());
            collector.emit(new Values(input));
        }
    }
    
    public static StormTopology createTopology(IPubSub pubSub) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new UrlSpout(pubSub));
        
        builder.setBolt("hostname", new AddHostname(), 5).shuffleGrouping("spout");
        // TODO do URL lengthening here.
        builder.setBolt("robots", new RobotsBolt(pubSub), 5).fieldsGrouping("hostname", new Fields("hostname"));
        builder.setBolt("fetch", new FetchUrlBolt(pubSub), 5).fieldsGrouping("robots", new Fields("ip"));
        builder.setBolt("parse", new ParsePageBolt(pubSub), 5).shuffleGrouping("fetch");
        
        // Get the parse results into S3
        builder.setBolt("store", new SavePageBolt(pubSub), 5).shuffleGrouping("parse-content");
        
        // Send the outlinks back to the crawlDB, to be merged in.
        builder.setBolt("links", new SaveLinksBolt(pubSub, 10000), 5).fieldsGrouping("parse-links", new Fields("outlink"));
        
        return builder.createTopology();
    }

}

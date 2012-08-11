package bixo.storm;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import bixo.robots.SimpleRobotRules;
import bixo.robots.SimpleRobotRules.RobotRulesMode;
import bixo.utils.DomainInfo;

@SuppressWarnings("serial")
public class RobotsBolt extends BaseKafkaBolt {
    private static final Logger LOGGER = Logger.getLogger(RobotsBolt.class);
    
    private transient Map<String, SimpleRobotRules> _robotRules;
    private transient Map<String, String> _ipAddresses;

    public RobotsBolt(KafkaTopics topics) {
        super(topics.getTopic(KafkaTopics.UPDATE_URLS_TOPIC_NAME));
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        
        _robotRules = new HashMap<String, SimpleRobotRules>();
        _ipAddresses = new HashMap<String, String>();
        
        // TODO allocate fetcher for getting robots.txt file, if needed.

    }
    

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "status", "hostname", "ip", "crawldelay"));
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String url = input.getStringByField("url");
        String status = input.getStringByField("status");
        String hostname = input.getStringByField("hostname");
        
        LOGGER.info("Process robots for " + url);
        
        if (!_robotRules.containsKey(hostname)) {
            try {
                URL realUrl = new URL(url);
                String hostAndProtocol = realUrl.getProtocol() + "://" + hostname;
                DomainInfo domainInfo = new DomainInfo(hostAndProtocol);
                
                LOGGER.info("Fetching robots.txt for " + hostAndProtocol);

                // TODO fetch robots.txt if we can
                _robotRules.put(hostname, new SimpleRobotRules(RobotRulesMode.ALLOW_ALL));
                _ipAddresses.put(hostname, domainInfo.getHostAddress());
            } catch (MalformedURLException e) {
                // We assume that we only have valid entries (invalid URLs have been
                // removed before this).
                // TODO 
                throw new RuntimeException("Impossible exception!", e);
            } catch (UnknownHostException e) {
                // TODO Need to publish to Kafka with appropriate status
                // TODO likely we should add entry to robots that causes it to be skipped,
                // so that we don't keep running into the same error over and over again.
                // Or maybe we need a new map for these cases (set of unknown host), and
                // we check that first
            } catch (URISyntaxException e) {
                throw new RuntimeException("Impossible exception!", e);
            }
        }
        
        SimpleRobotRules rules = _robotRules.get(hostname);
        
        // See if we need to filter out this URL.
        if (rules == null) {
            // TODO remove this once we fix up UnknownHostException handlign above.
            _publisher.publish(new UrlDatum(url, "invalid"));
        } else if (rules.isDeferVisits()) {
            _publisher.publish(new UrlDatum(url, "deferred"));
            // TODO ack immediately
        } else if (!rules.isAllowed(url)) {
            _publisher.publish(new UrlDatum(url, "blocked"));
            // TODO ack immediately
        } else {
            collector.emit(new Values(url, status, hostname, _ipAddresses.get(hostname), rules.getCrawlDelay()));
        }
    }
}

package bixo.storm.bolt;

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
import bixo.fetcher.SimpleHttpFetcher;
import bixo.robots.SimpleRobotRules;
import bixo.robots.SimpleRobotRules.RobotRulesMode;
import bixo.storm.BixoConfig;
import bixo.storm.CrawlDBClient;
import bixo.storm.DomainResolver;
import bixo.storm.UrlStatus;

@SuppressWarnings("serial")
public class RobotsBolt extends BaseBasicBolt {
    private static final Logger LOGGER = Logger.getLogger(RobotsBolt.class);
    
    private CrawlDBClient _cdbClient;
    private SimpleHttpFetcher _robotsFetcher;
    private DomainResolver _domainResolver;
    
    private transient Map<String, SimpleRobotRules> _robotRules;
    private transient Map<String, String> _ipAddresses;
    
    public RobotsBolt(BixoConfig config) {
        super();
        
        _cdbClient = config.getCdbClient();
        _robotsFetcher = config.getRobotsFetcher();
        _domainResolver = config.getDomainResolver();
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        
        _robotRules = new HashMap<String, SimpleRobotRules>();
        _ipAddresses = new HashMap<String, String>();
    }
    
    @Override
    public void cleanup() {
        _cdbClient.shutdown();
        
        super.cleanup();
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
                String ipAddress = _domainResolver.getIpAddress(realUrl.getProtocol(), hostname);
                
                LOGGER.info("Fetching robots.txt for " + url);

                // TODO fetch robots.txt if we can
                _robotRules.put(hostname, new SimpleRobotRules(RobotRulesMode.ALLOW_ALL));
                _ipAddresses.put(hostname, ipAddress);
            } catch (MalformedURLException e) {
                // Shouldn't happen, but...
                _cdbClient.updateUrlQuietly(url, UrlStatus.INVALID_URL);
            } catch (UnknownHostException e) {
                _cdbClient.updateUrlQuietly(url, UrlStatus.UNKNOWN_HOST);

                // TODO likely we should add entry to robots that causes it to be skipped,
                // so that we don't keep running into the same error over and over again.
                // Or maybe we need a new map for these cases (set of unknown host), and
                // we check that first
            } catch (URISyntaxException e) {
                // Shouldn't happen, but...
                _cdbClient.updateUrlQuietly(url, UrlStatus.INVALID_URL);
            }
        }
        
        SimpleRobotRules rules = _robotRules.get(hostname);
        
        // See if we need to filter out this URL.
        if (rules == null) {
            // We weren't able to get/process robots.txt for the url for some reason.
            
        } else if (rules.isDeferVisits()) {
            _cdbClient.updateUrlQuietly(url, UrlStatus.DEFERRED_BY_ROBOTS);
            // TODO ack immediately
        } else if (!rules.isAllowed(url)) {
            _cdbClient.updateUrlQuietly(url, UrlStatus.BLOCKED_BY_ROBOTS);
            // TODO ack immediately
        } else {
            collector.emit(new Values(url, status, hostname, _ipAddresses.get(hostname), rules.getCrawlDelay()));
        }
    }
}

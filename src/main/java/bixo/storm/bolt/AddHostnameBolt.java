package bixo.storm.bolt;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import bixo.storm.BixoConfig;
import bixo.storm.CrawlDBClient;
import bixo.storm.UrlStatus;

@SuppressWarnings("serial")
public class AddHostnameBolt extends BaseBasicBolt {
    
    private CrawlDBClient _cdbClient;
    
    public AddHostnameBolt(BixoConfig config) {
        _cdbClient = config.getCdbClient();
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "status", "hostname"));
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String url = input.getStringByField("url");
        
        try {
            URL realUrl = new URL(url);
            String hostname = realUrl.getHost();
            String status = input.getStringByField("status");
            collector.emit(new Values(url, status, hostname));
        } catch (MalformedURLException e) {
            _cdbClient.updateUrlQuietly(url, UrlStatus.INVALID_URL);
            // TODO ack immediately
        }
    }


}

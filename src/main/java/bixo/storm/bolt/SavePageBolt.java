package bixo.storm.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class SavePageBolt implements IRichBolt {

    private transient OutputCollector _collector;
    
    private IPubSub _publisher;
    
    public SavePageBolt(IPubSub publisher) {
        super();
        
        _publisher = publisher;
    }
    

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        // TODO Save the resulting page
        _collector.ack(input);
    }

    @Override
    public void cleanup() {
        // Nothing to clean up
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // We don't emit any results
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // No special configuration
        return null;
    }

}

package bixo.storm;

import java.util.LinkedHashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class SaveLinksBolt implements IRichBolt {

    private transient OutputCollector _collector;
    private transient LinkedHashMap<String, Object> _tuples;
    private int _threshold;
    
    public SaveLinksBolt(int threshold) {
        _threshold = threshold;
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        
        _tuples = new LinkedHashMap<String, Object>(_threshold, 0.75f, true) {
            
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Object> eldest) {
              if (size() > _threshold) {
                  sendOutlink(eldest.getKey());
                  return true;
              } else {
                  return false;
              }
            }

        };
    }

    private void sendOutlink(String outlink) {
        // TODO Send via Kafka
    }

    @Override
    public void execute(Tuple input) {
        String outlink = input.getStringByField("outlink");
        
        // Dedup outlinks, as we often get a bunch during a fetch cycle
        // from a set of pages in the same domain (e.g. links to about page).
        if (!_tuples.containsKey(outlink)) {
            _tuples.put(outlink, null);
        }

        _collector.ack(input);
    }

    @Override
    public void cleanup() {
        for (String outlink : _tuples.keySet()) {
            sendOutlink(outlink);
        }
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

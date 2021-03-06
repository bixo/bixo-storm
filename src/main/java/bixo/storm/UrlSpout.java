package bixo.storm;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

@SuppressWarnings("serial")
public class UrlSpout extends BaseRichSpout {
    private static final Logger LOGGER = Logger.getLogger(UrlSpout.class);
    
    private static final int MAX_QUEUED_URLS = 1000;

    private final KafkaTopic _subscriber;
    
    protected transient SpoutOutputCollector _collector;
    private transient LinkedBlockingQueue<UrlDatum> _queue = null;
    
    public UrlSpout(KafkaTopics topics) {
        super();
        
        _subscriber = topics.getTopic(KafkaTopics.FETCH_URLS_TOPIC_NAME);
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _queue = new LinkedBlockingQueue<UrlDatum>(MAX_QUEUED_URLS);

        // We're already running in a thread (handled by Storm) so we're OK even
        // though the call to Kafka will block.
        for (UrlDatum url : _subscriber) {
            LOGGER.info("Consumed URL from Kafka: " + url);
            // TODO what to do when the queue is full? Just spin here until
            // it becomes empty?
            _queue.offer(url);
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config result = new Config();
        result.setMaxTaskParallelism(1);
        return result;
    }

    @Override
    public void nextTuple() {
        UrlDatum url = _queue.poll();
        if (url == null) {
            Utils.sleep(50);
        } else {
            _collector.emit(new Values(url.getUrl(), url.getStatus()));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "status"));
    }
    
}

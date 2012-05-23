package bixo.storm;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaMessageStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import bixo.utils.ThreadedExecutor;

@SuppressWarnings("serial")
public class UrlSpout extends BaseRichSpout {
    private static final Logger LOGGER = Logger.getLogger(UrlSpout.class);
    
    private static final int MAX_QUEUED_URLS = 1000;

    protected transient SpoutOutputCollector _collector;
    private transient LinkedBlockingQueue<UrlDatum> _queue = null;
    private transient ThreadedExecutor _executor;

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        
        _queue = new LinkedBlockingQueue<UrlDatum>(MAX_QUEUED_URLS);

        _executor = new ThreadedExecutor(1, CrawlerConfig.MAX_CONSUMER_DURATION);

        // Set up to be a Kafka consumer, handling URL fetch requests.
        Properties consumerProps = new Properties();
        consumerProps.put("groupid", CrawlerConfig.KAFKA_GROUP_ID);
        
        // TODO - figure out what's really needed here.
        // consumerProps.put("consumer.timeout.ms", "" + MAX_CONSUMER_DURATION);
        consumerProps.put("zk.connect", "127.0.0.1:2181");
        consumerProps.put("zk.connectiontimeout.ms", "1000000");
        consumerProps.put("broker.list", "0:localhost:9092");

        // Create the connection to the cluster
        ConsumerConfig consumerConfig = new ConsumerConfig(consumerProps);
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

        Map<String, Integer> streamInfo = new HashMap<String, Integer>();
        streamInfo.put(CrawlerConfig.KAFKA_FETCH_TOPIC, 1);
        
        List<KafkaMessageStream<Message>> streams = consumerConnector.createMessageStreams(streamInfo).get(CrawlerConfig.KAFKA_FETCH_TOPIC);
        for (final KafkaMessageStream<Message> stream : streams) {
            _executor.execute(new Runnable() {
                public void run() {
                    UrlDatumDecoder decoder = new UrlDatumDecoder();
                    
                    for (Message message : stream) {
                            UrlDatum newUrl = decoder.toEvent(message);
                            LOGGER.info("Consumed URL from Kafka: " + newUrl);
                            // TODO what to do when the queue is full? Just spin here until
                            // it becomes empty?
                            _queue.offer(newUrl);
                    }
                }
            });
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

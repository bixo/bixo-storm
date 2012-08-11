package bixo.storm;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Container for PubSub "topics"
 * 
 */
@SuppressWarnings("serial")
public class KafkaTopics implements Serializable {

    public static final String FETCH_URLS_TOPIC_NAME = "urls-to-fetch";
    public static final String UPDATE_URLS_TOPIC_NAME = "urls-to-update";
    public static final String OUTLINK_URLS_TOPIC_NAME = "urls-to-add";
    
    public static final long MAX_CONSUMER_DURATION = 10000;

    private Map<String, KafkaTopic> _topics;

    public KafkaTopics() {
        _topics = Collections.synchronizedMap(new HashMap<String, KafkaTopic>());
    }
    
    public void addTopic(KafkaTopic topic) {
        _topics.put(topic.getTopicName(), topic);
    }
    
    public KafkaTopic getTopic(String topicName) {
        return _topics.get(topicName);
    }
    
    public static KafkaTopics getDefaultTopics() {
        KafkaTopics topics = new KafkaTopics();
        topics.addTopic(new KafkaTopic(KafkaTopics.FETCH_URLS_TOPIC_NAME));
        topics.addTopic(new KafkaTopic(KafkaTopics.UPDATE_URLS_TOPIC_NAME));
        topics.addTopic(new KafkaTopic(KafkaTopics.OUTLINK_URLS_TOPIC_NAME));
        return topics;
    }
    
}

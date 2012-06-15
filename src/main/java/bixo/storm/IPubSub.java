package bixo.storm;

/**
 * PubSub interface for UrlDatum's, for a single topic (in Kafka terminology)
 * 
 */
public interface IPubSub {

    public static final String FETCH_URLS_TOPIC_NAME = "urls-to-fetch";
    public static final String UPDATE_URLS_TOPIC_NAME = "urls-to-update";
    public static final String OUTLINK_URLS_TOPIC_NAME = "urls-to-add";
    
    public void addTopic(BasePubSubTopic topic);
    
    public BasePubSubTopic getTopic(String topicName);
}

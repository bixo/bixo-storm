package bixo.storm;

import java.io.Serializable;

@SuppressWarnings("serial")
public abstract class BasePubSubTopic implements Iterable<UrlDatum>, Serializable {

    protected String _topicName;
    
    public BasePubSubTopic(String topic) {
        _topicName = topic;
    }
    
    public String getTopicName() {
        return _topicName;
    }

    // Publish a URL to the topic
    public abstract void publish(UrlDatum url);
    
    public abstract void close();

}

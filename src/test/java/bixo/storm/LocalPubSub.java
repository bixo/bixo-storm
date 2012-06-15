package bixo.storm;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class LocalPubSub implements IPubSub, Serializable {

    // We make this static so that we get shared state across all of
    // the threads that Storm fires up.
    private static Map<String, BasePubSubTopic> _topics;
    
    static {
        _topics = Collections.synchronizedMap(new HashMap<String, BasePubSubTopic>());
    }
    
    public LocalPubSub() {
    }
    
    @Override
    public void addTopic(BasePubSubTopic topic) {
        _topics.put(topic.getTopicName(), topic);
    }

    @Override
    public BasePubSubTopic getTopic(String topicName) {
        return _topics.get(topicName);
    }

}

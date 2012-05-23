package bixo.storm;

public interface CrawlerConfig {

    public static final String KAFKA_GROUP_ID = "bixo-storm";
    public static final String KAFKA_UPDATE_TOPIC = "update-url";
    public static final String KAFKA_FETCH_TOPIC = "fetch-url";
    
    public static final long MAX_CONSUMER_DURATION = 10000;
    

}

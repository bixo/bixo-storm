package bixo.storm;

/**
 * PubSub interface for UrlDatum's, for a single topic (in Kafka terminology)
 * 
 */
public interface IUrlDatumPubSub extends Iterable<UrlDatum> {

    // Publish a URL to the topic
    public void publish(UrlDatum url);
}

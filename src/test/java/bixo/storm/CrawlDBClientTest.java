package bixo.storm;

import org.cassandraunit.AbstractCassandraUnit4TestCase;
import org.cassandraunit.dataset.DataSet;
import org.cassandraunit.dataset.xml.ClassPathXmlDataSet;
import org.junit.Test;

public class CrawlDBClientTest extends AbstractCassandraUnit4TestCase {

    @Override
    public DataSet getDataSet() {
        return new ClassPathXmlDataSet("cassandra_urls_keyspace_empty.xml");
    }

    @Test
    public void test() throws Exception {
        CrawlDBClient client = new CrawlDBClient(new CassandraConfig("localhost", 9171));
        
        client.addUrl("http://www.cnn.com", 1.0);
        client.updateUrl(new UrlDatum("http://www.cnn.com", UrlStatus.FETCHED, System.currentTimeMillis(), System.currentTimeMillis(), 1.0));
    }

}

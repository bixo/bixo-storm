package bixo.storm;

import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ExceptionCallback;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

/* 
 * Spout that reads entries from a Cassandra database.
 * 
 * There's a continuous process running that adds URLs to the queue on a per-
 * partition basis, and avoids getting the next partition until the previous partition's
 * results have been added to the queue.
 * 
 * This requires that the URL entry in Cassandra has a composite key, where the first
 * (primary) column for the key is the partitioning value (an integer), and the second
 * column for the key is the long hash of the normalized URL. Cassandra will ensure that
 * all rows with the same partitioning value are on the same node, which improves
 * performance by avoiding reads that are scattered across all nodes.
*/

@SuppressWarnings("serial")
public class UrlSpout extends BaseRichSpout {
    private static final Logger LOGGER = Logger.getLogger(UrlSpout.class);
    
    private static final int MAX_URLS_IN_QUEUE = 100;
    private static final int URLS_PER_REQUEST = 100;
    
    private static final int MAX_CONNECTION_FAILURES = 1;

    // Let a URL be in the fetching state for a day before we automatically clear it.
    private static final long MAX_FETCHING_STATE_DURATION = 24 * 3600 * 1000L;

    private CrawlDBClient _cdbClient;
    
    private transient SpoutOutputCollector _collector;
    private transient Queue<UrlDatum> _queue;
    private transient Thread _queueLoader;
    private transient boolean _active;
    
    public UrlSpout(BixoConfig config) {
        super();
        
        
        _cdbClient = config.getCdbClient();
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;

        _queue = new PriorityQueue<UrlDatum>();
        
        // We need to start up a thread that's constantly adding fetchable URLs to the
        // queue.
        Runnable loadQueue = new Runnable() {
            
            @Override
            public void run() {
                final Keyspace keyspace = _cdbClient.getKeyspace();
                
                _active = true;
                
                int numFailures = 0;
                while (!Thread.interrupted()) {
                    // First make sure we've got space in the queue.
                    if (_queue.size() >= MAX_URLS_IN_QUEUE) {
                        try {
                            Thread.sleep(1000L);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        
                        continue;
                    }
                    
                    try {
                        OperationResult<Rows<String, String>> rows = keyspace.prepareQuery(CassandraUtils.CF_URLS)
                                        .getAllRows()
                                        .setRowLimit(URLS_PER_REQUEST)  // This is the page size
                                        .setExceptionCallback(new ExceptionCallback() {

                                            @Override
                                            public boolean onException(ConnectionException e) {
                                                // TODO Auto-generated method stub
                                                return false;
                                            }
                                        })
                                        .execute();

                        Iterator<Row<String, String>> iter = rows.getResult().iterator();
                        while ((_queue.size() < MAX_URLS_IN_QUEUE) && iter.hasNext()) {
                            Row<String, String> row = iter.next();
                            ColumnList<String> columns = row.getColumns();
                            UrlStatus status = UrlStatus.fromInt(columns.getIntegerValue(CassandraUtils.STATUS_COLUMN_NAME, UrlStatus.UNFETCHED.ordinal()));
                            
                            if (status.isPermError()) {
                                // ignore it
                                continue;
                            }
                            
                            long statusTime = columns.getLongValue(CassandraUtils.STATUS_TIME_COLUMN_NAME, 0L);
                            if (status.isFetching() && (statusTime + MAX_FETCHING_STATE_DURATION > System.currentTimeMillis())) {
                                continue;
                            }
                            
                            long fetchTime = columns.getLongValue(CassandraUtils.FETCH_TIME_COLUMN_NAME, 0L);
                            if (fetchTime > System.currentTimeMillis()) {
                                continue;
                            }
                            
                            // Now we know that we want to fetch this particular URL. Let's update it in Cassandra, and then add it
                            // to the queue.
                            
                            String url = row.getKey();
                            double score = columns.getDoubleValue(CassandraUtils.SCORE_COLUMN_NAME, UrlDatum.DEFAULT_SCORE);
                            UrlDatum urlDatum = new UrlDatum(url, UrlStatus.FETCHING, System.currentTimeMillis(), fetchTime, score);
                            
                            MutationBatch m = keyspace.prepareMutationBatch();
                            m.withRow(CassandraUtils.CF_URLS, url)
                            .putColumn(CassandraUtils.STATUS_COLUMN_NAME, UrlStatus.FETCHING.ordinal(), null)
                            .putColumn(CassandraUtils.STATUS_TIME_COLUMN_NAME, urlDatum.getStatusTime(), null)
                            .putColumn(CassandraUtils.FETCH_TIME_COLUMN_NAME, urlDatum.getFetchTime(), null)
                            .putColumn(CassandraUtils.SCORE_COLUMN_NAME, urlDatum.getScore(), null);
                            m.execute();

                            _queue.add(urlDatum);
                        }
                        
                        numFailures = 0;
                    } catch (ConnectionException e) {
                        numFailures += 1;
                        if (numFailures > MAX_CONNECTION_FAILURES) {
                            // TODO KKr - record failure somewhere
                            break;
                        }
                    }
                }
                
                _active = false;
            }
        };
        
        _queueLoader = new Thread(loadQueue, "UrlSpout queue loader");
        _queueLoader.start();
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
        // TODO add full set of fields
        // TODO use named constants for fields.
        declarer.declare(new Fields("url", "status"));
    }
    
    @Override
    public void close() {
        _queueLoader.interrupt();
        
        // TODO - what about timeout here?
        while (_active && !Thread.interrupted()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // TODO - what to do here?
                Thread.currentThread().interrupt();
            }
        }
        
        _cdbClient.shutdown();

        super.close();
    }

}

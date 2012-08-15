package bixo.storm;

import org.apache.log4j.Logger;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

/**
 * Client that supports add/update of entries in Cassandra, using the
 * Astyanax client.
 * 
 */
public class CrawlDBClient {
    private static final Logger LOGGER = Logger.getLogger(CrawlDBClient.class);
    
    private CassandraConfig _config;
    
    private transient AstyanaxContext<Keyspace> _context;
    private transient Keyspace _keyspace;
    
    public CrawlDBClient(CassandraConfig config) {
        _config = config;
    }
    
    private void init() {
        if (_context == null) {
            _context = CassandraUtils.createContext(_config);
            _context.start();
            _keyspace = _context.getEntity();        
        }
    }
    
    public Keyspace getKeyspace() {
        init();
        
        return _keyspace;
    }
    
    public void shutdown() {
        if (_context != null) {
            _context.shutdown();
        }
    }
    
    
    public void addUrl(String url, double score) throws ConnectionException {
        init();
        
        // TODO KKr - should we re-use the MutationBatch?
        MutationBatch m = _keyspace.prepareMutationBatch();
        
        // Set timestamp to 0 so we won't update an existing entry.
        m.setTimestamp(0);
        
        // hash of normalized URL
        //        Raw URL
        //        Status
        //        Status time
        //        Next fetch time
        //        Content hash
        //        S3 reference

        m.withRow(CassandraUtils.CF_URLS,url)
        .putColumn(CassandraUtils.STATUS_COLUMN_NAME, UrlStatus.UNFETCHED.ordinal(), null)
        .putColumn(CassandraUtils.STATUS_TIME_COLUMN_NAME, System.currentTimeMillis(), null)
        .putColumn(CassandraUtils.FETCH_TIME_COLUMN_NAME, System.currentTimeMillis(), null)
        .putColumn(CassandraUtils.SCORE_COLUMN_NAME, score, null);

        m.execute();
    }
    
    
    /**
     * Version of updateUrl that doesn't throw exceptions, since in most cases we'd be OK with the
     * state not getting changed, but we don't want to have to catch/handle exceptions everywhere.
     * 
     * @param url
     * @param status
     */
    public void updateUrlQuietly(String url, UrlStatus status) {
        try {
            updateUrl(url, status, System.currentTimeMillis(), UrlDatum.UNSET_FETCH_TIME, UrlDatum.UNSET_SCORE);
        } catch (ConnectionException e) {
            LOGGER.error(String.format("Exception updating %s to status %s in CrawlDB", url, status), e);
        }
    }
    
    public void updateUrl(String url, UrlStatus status) throws ConnectionException {
        updateUrl(url, status, System.currentTimeMillis(), UrlDatum.UNSET_FETCH_TIME, UrlDatum.UNSET_SCORE);
    }
    
    public void updateUrl(UrlDatum url) throws ConnectionException {
        updateUrl(url.getUrl(), url.getStatus(), url.getStatusTime(), url.getFetchTime(), url.getScore());
    }
    
    public void updateUrl(String url, UrlStatus status, long statusTime, long fetchTime, double score) throws ConnectionException {
        init();
        
        // TODO KKr - should we re-use the MutationBatch?
        MutationBatch m = _keyspace.prepareMutationBatch();
        
        // hash of normalized URL
        //        Raw URL
        //        Status
        //        Status time
        //        Next fetch time
        //        Content hash
        //        S3 reference

        ColumnListMutation<String> clm = m.withRow(CassandraUtils.CF_URLS, url);
        clm.putColumn(CassandraUtils.STATUS_COLUMN_NAME, status.ordinal(), null);
        clm.putColumn(CassandraUtils.STATUS_TIME_COLUMN_NAME, statusTime, null);
        
        if (fetchTime != UrlDatum.UNSET_FETCH_TIME) {
            clm.putColumn(CassandraUtils.FETCH_TIME_COLUMN_NAME, fetchTime, null);
        }
        
        if (score != UrlDatum.UNSET_SCORE) {
            clm.putColumn(CassandraUtils.SCORE_COLUMN_NAME, score, null);
        }
        
        m.execute();
    }
    

    
}

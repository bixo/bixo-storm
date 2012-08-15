package bixo.storm;

/**
 * UrlDatum contains information about one URL
 *
 */
public class UrlDatum implements Comparable<UrlDatum> {

    public static final long UNSET_FETCH_TIME = Long.MIN_VALUE;
    public static final double UNSET_SCORE = Double.NEGATIVE_INFINITY;

    public static final double DEFAULT_SCORE = 1.0;
    
    private String _url;
    private UrlStatus _status;
    private long _statusTime;
    private long _fetchTime;
    private double _score;
    
    public UrlDatum(String url, UrlStatus status, long statusTime, long fetchTime, double score) {
        _url = url;
        _status = status;
        _statusTime = statusTime;
        _fetchTime = fetchTime;
        _score = score;
    }

    public String getUrl() {
        return _url;
    }

    public void setUrl(String url) {
        _url = url;
    }

    public UrlStatus getStatus() {
        return _status;
    }

    public void setStatus(UrlStatus status) {
        _status = status;
    }

    public long getStatusTime() {
        return _statusTime;
    }
    
    public void setStatusTime(long statusTime) {
        _statusTime = statusTime;
    }
    
    public long getFetchTime() {
        return _fetchTime;
    }
    
    public void setFetchTime(long fetchTime) {
        _fetchTime = fetchTime;
    }
    
    public double getScore() {
        return _score;
    }
    
    public void setScore(double score) {
        _score = score;
    }
    
    @Override
    public String toString() {
        return String.format("%s: %s", _status, _url);
    }

    /* (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     * 
     * Return ordering by score, from high to low.
     */
    @Override
    public int compareTo(UrlDatum o) {
        if (_score > o._score) {
            return -1;
        } else if (_score < o._score) {
            return 1;
        } else {
            // TODO KKr - order by fetch time
            return 0;
        }
    }
    
}

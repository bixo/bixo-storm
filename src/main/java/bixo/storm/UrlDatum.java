package bixo.storm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;

/**
 * UrlDatum needs to be writable for our Kafka serialization, and
 * serializable for our LocalPubSubTopic.
 * 
 * @author kenkrugler
 *
 */
@SuppressWarnings("serial")
public class UrlDatum implements Serializable, Writable {

    // TODO change status to enum (need to update UrlDatumEncoder/decoder, etc)
    // TODO add status time field.
    
    private String _url;
    private String _status;
    
    public UrlDatum() {
        // Empty constructor for deserialization
    }
    
    public UrlDatum(String url, String status) {
        _url = url;
        _status = status;
    }

    public String getUrl() {
        return _url;
    }

    public void setUrl(String url) {
        _url = url;
    }

    public String getStatus() {
        return _status;
    }

    public void setStatus(String status) {
        _status = status;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((_url == null) ? 0 : _url.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        UrlDatum other = (UrlDatum) obj;
        if (_url == null) {
            if (other._url != null)
                return false;
        } else if (!_url.equals(other._url))
            return false;
        return true;
    }
    
    @Override
    public String toString() {
        return String.format("%s %s", _status, _url);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        _url = in.readUTF();
        _status = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(_url);
        out.writeUTF(_status);
    }
}

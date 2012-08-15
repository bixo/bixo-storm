package bixo.storm;

public class CassandraConfig {

    private String _hostname;
    private int _port;
    
    public CassandraConfig(String hostname, int port) {
        _hostname = hostname;
        _port = port;
    }

    public String getHostname() {
        return _hostname;
    }

    public void setHostname(String hostname) {
        _hostname = hostname;
    }

    public int getPort() {
        return _port;
    }

    public void setPort(int port) {
        _port = port;
    }
    
    
}

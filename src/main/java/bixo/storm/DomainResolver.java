package bixo.storm;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

public class DomainResolver {
    
    public String getIpAddress(String protocol, String hostname) throws UnknownHostException, URISyntaxException {
        // Since URI class is stricter than URL when validating, let's use that to validate.
        URI uri = new URI(protocol, hostname, null);
        
        return InetAddress.getByName(uri.getHost()).getHostAddress();
    }
}

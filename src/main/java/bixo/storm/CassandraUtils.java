package bixo.storm;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class CassandraUtils {

    public static final String STATUS_COLUMN_NAME = "status";
    public static final String STATUS_TIME_COLUMN_NAME = "status-time";
    public static final String FETCH_TIME_COLUMN_NAME = "fetch-time";
    public static final String SCORE_COLUMN_NAME = "score";

    private static final String CLUSTER_NAME = "bixo-storm";
    private static final String KEYSPACE_NAME = "urls";
    private static final String COLUMN_FAMILY_NAME = "urls";

    // TODO KKr - we need composite key support.
    public static final ColumnFamily<String, String> CF_URLS = new ColumnFamily<String, String>(
                                    COLUMN_FAMILY_NAME,             // Column Family Name
                                    StringSerializer.get(),         // Key serializer
                                    StringSerializer.get());        // Column name serializer


    /**
     * Create (unstarted) Cassandra context
     * 
     * @param config - provide hostname, port, etc.
     * @return configuration usable for creating keyspaces, which in turn can be
     *         used to get/set rows in a Cassandra DB.
     */
    public static AstyanaxContext<Keyspace> createContext(CassandraConfig config) {
        AstyanaxContext<Keyspace> result = new AstyanaxContext.Builder()
        .forCluster(CLUSTER_NAME)
        .forKeyspace(KEYSPACE_NAME)
        .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()      
            .setDiscoveryType(NodeDiscoveryType.NONE)
         )
    .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
        .setPort(config.getPort())
        .setMaxConnsPerHost(10)
        .setSeeds(config.getHostname())
    )
    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
    .buildKeyspace(ThriftFamilyFactory.getInstance());

        return result;
    }
}

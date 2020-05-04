package org.apache.gobblin.connector.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.TlsPolicy;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Aerospike local client class to connect to Aerospike database.
 * <p>
 * Reader or Writer will require to extend this class to get the Aerospike client
 * </p>
 */
/*

 */
public class AerospikeLocalClient {

    public static final Logger LOG = LoggerFactory.getLogger(AerospikeLocalClient.class);

    protected final AerospikeConfig aeroConf;
    protected final AerospikeClient aeroClient;
    protected final ClientPolicy clientPolicy = new ClientPolicy();

    /**
     * Number of records successfully Read/Written
     */
    protected final AtomicLong count = new AtomicLong(0);
    /**
     * Number of Bytes successfully Read/Written.
     */
    protected final AtomicLong bytesCount = new AtomicLong(0);

    /**
     * Instantiates a new Aerospike writer.
     */
    public AerospikeLocalClient(AerospikeConfig aeroConf) {
        try {
            this.aeroConf = aeroConf;
            Host[] host_list = new Host[aeroConf.hosts.size()];
            String tlsHostName = aeroConf.tlsHost;
            for (int i = 0; i < aeroConf.hosts.size(); i++) {
                host_list[i] = new Host(aeroConf.hosts.get(i), tlsHostName, aeroConf.port);
            }

            //set up aerospike credentials
            clientPolicy.user = aeroConf.user;
            clientPolicy.password = aeroConf.password;
            if (aeroConf.isTLSEnabled) {
                clientPolicy.tlsPolicy = new TlsPolicy();
            }

            // required parameter checks
            Preconditions.checkNotNull(clientPolicy.user);
            Preconditions.checkNotNull(clientPolicy.password);
            Preconditions.checkArgument(aeroConf.keyColumns.size() > 0);
            Preconditions.checkArgument(aeroConf.binColumns.size() > 0);

            this.aeroClient = new AerospikeClient(clientPolicy, host_list);
            while (!this.aeroClient.isConnected()) {
                Thread.sleep(1000);
            }

        } catch (AerospikeException e) {
            throw new AerospikeException("Error creating writer", e);
        } catch (InterruptedException e) {
            throw new AerospikeException("Failed to connect to Aerospike cluster", e);
        }
    }

}

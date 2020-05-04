package org.apache.gobblin.connector.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.google.common.base.Preconditions;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.writer.DataWriter;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;

/**
 * Aerospike writer class to write key value data to Aerospike database.
 * <p>
 * Features
 * 1. Basic key value writer
 * 2. Abstract writer with implementation with support for Avro schema
 * 3. Sync/Async write modes
 * 4. Throttling integration
 * 5. Unsecure and secure connection handling
 * 6. Error & retries handling ( retry queue for data delivery SLA )
 * 7. Metrics integration specific to the writer
 */
/*

 */
public class AerospikeAvroWriter implements DataWriter<GenericRecord> {

    public static final Logger LOG = LoggerFactory.getLogger(AerospikeAvroWriter.class);

    private final AerospikeConfig aeroConf;
    private final AerospikeClient client;
    private final WritePolicy writePolicy = new WritePolicy();
    private final ClientPolicy clientPolicy = new ClientPolicy();

    /**
     * Number of records successfully written
     */
    protected final AtomicLong count = new AtomicLong(0);
    /**
     * Number of Bytes successfully written.
     */
    protected final AtomicLong bytesCount = new AtomicLong(0);

    /**
     * Instantiates a new Aerospike writer.
     */
    public AerospikeAvroWriter(AerospikeConfig aeroConf, Schema schema) {
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

            this.client = new AerospikeClient(clientPolicy, host_list);
            while (!this.client.isConnected()) {
                Thread.sleep(1000);
            }

        } catch (AerospikeException e) {
            throw new AerospikeException("Error creating writer", e);
        } catch (InterruptedException e) {
            throw new AerospikeException("Failed to connect to Aerospike cluster", e);
        }
    }

    @Override
    public void write(GenericRecord record) {
        StringBuffer keyBuffer = new StringBuffer();
        StringBuffer binBuffer = new StringBuffer();

        // combined key columns
        for (int i = 0; i < aeroConf.keyColumns.size(); i++) {
            if (i > 0) {
                keyBuffer.append(aeroConf.keySeparator);
            }
            keyBuffer.append(record.get(aeroConf.keyColumns.get(i)));
        }

        // combined bin(value) columns
        for (int i = 0; i < aeroConf.binColumns.size(); i++) {
            if (i > 0) {
                binBuffer.append(aeroConf.binSeparator);
            }
            binBuffer.append(record.get(aeroConf.binColumns.get(i)));
        }

        Key key = new Key(aeroConf.namespace, aeroConf.set, keyBuffer.toString());
        Bin bin = new Bin(aeroConf.bin, binBuffer.toString());
        client.put(writePolicy, key, bin);
        bytesCount.updateAndGet(n -> n + (keyBuffer.length() + binBuffer.length()));
        this.count.incrementAndGet();
    }

    @Override
    public void commit() {
        LOG.info("No commit required for Aerospike.");
    }

    @Override
    public void cleanup() {
        LOG.info("No cleanup to do for now.");
    }

    @Override
    public long recordsWritten() {
        return this.count.get();
    }

    @Override
    public long bytesWritten() {
        return bytesCount.get();
    }

    @Override
    public void close() {
        this.client.close();
    }

}
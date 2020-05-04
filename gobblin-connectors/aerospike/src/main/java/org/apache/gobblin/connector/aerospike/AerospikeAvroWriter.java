package org.apache.gobblin.connector.aerospike;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.writer.DataWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Aerospike writer class to write key value data to Aerospike database using {@link AerospikeLocalClient}.
 * <p>
 * Features (TODO)
 * 1. Basic key value writer
 * 2. Abstract writer with implementation with support for Avro schema
 * 3. Sync/Async write modes
 * 4. Throttling integration
 * 5. Unsecure and secure connection handling
 * 6. Error & retries handling ( retry queue for data delivery SLA )
 * 7. Metrics integration specific to the writer
 * </p>
 */
/*

 */
public class AerospikeAvroWriter extends AerospikeLocalClient implements DataWriter<GenericRecord> {

    public static final Logger LOG = LoggerFactory.getLogger(AerospikeAvroWriter.class);

    private final WritePolicy writePolicy = new WritePolicy();

    public AerospikeAvroWriter(AerospikeConfig aeroConf, Schema schema) {
        super(aeroConf);
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
        aeroClient.put(writePolicy, key, bin);
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
        this.aeroClient.close();
    }

}
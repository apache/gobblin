package org.apache.gobblin.connector.aerospike;

import com.typesafe.config.Config;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.writer.DataWriterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class AerospikeWriterBuilder extends DataWriterBuilder<Schema, GenericRecord> {

    @Override
    public AerospikeAvroWriter build() {
        State state = this.destination.getProperties();
        Properties taskProps = state.getProperties();
        Config config = ConfigUtils.propertiesToConfig(taskProps);

        AerospikeConfig aerospikeConfig = new AerospikeConfig(config);
        return new AerospikeAvroWriter(aerospikeConfig, this.getSchema());
    }

    @Override
    public Schema getSchema() {
        return super.getSchema();
    }

}
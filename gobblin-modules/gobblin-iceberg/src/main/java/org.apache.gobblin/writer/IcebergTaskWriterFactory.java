package org.apache.gobblin.writer;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.*;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import java.util.Map;

/**
 * Factory to create {@link TaskWriter} that is.
 * responsible for writing generic data
 * into Iceberg table.
 *
 */
public class IcebergTaskWriterFactory {

    private final Schema schema;
    private final PartitionSpec partitionSpec;
    private final LocationProvider locations;
    private final FileIO io;
    private final EncryptionManager encryptionManager;
    private final long targetFileSizeBytes;
    private final FileFormat format;
    private final IcebergFileAppenderFactory appenderFactory;

    private transient OutputFileFactory outputFileFactory;

    public IcebergTaskWriterFactory(Schema schema,
                                    PartitionSpec partitionSpec,
                                    LocationProvider locations,
                                    FileIO io,
                                    EncryptionManager encryptionManager,
                                    long targetFileSizeBytes,
                                    FileFormat format,
                                    Map<String, String> tableProperties) {
        this.schema = schema;
        this.partitionSpec = partitionSpec;
        this.locations = locations;
        this.io = io;
        this.encryptionManager = encryptionManager;
        this.targetFileSizeBytes = targetFileSizeBytes;
        this.format = format;
        this.appenderFactory = new IcebergFileAppenderFactory(schema);
    }

    public void initialize(int taskId, int attemptId) {
        this.outputFileFactory = new OutputFileFactory(partitionSpec, format, locations, io, encryptionManager, taskId, attemptId);
    }

    public TaskWriter create() {
        Preconditions.checkNotNull(outputFileFactory,
                "The outputFileFactory shouldn't be null if we have invoked the initialize().");

        if (partitionSpec.fields().isEmpty()) {
            return new UnpartitionedWriter(partitionSpec, format, appenderFactory, outputFileFactory, io, targetFileSizeBytes);
        } else {
            // Also use unpartitioned writer, since the partition should be done in source
            return new UnpartitionedWriter(partitionSpec, format, appenderFactory, outputFileFactory, io, targetFileSizeBytes);
        }
    }

}

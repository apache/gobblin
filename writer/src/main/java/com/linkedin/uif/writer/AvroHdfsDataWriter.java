package com.linkedin.uif.writer;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.linkedin.uif.converter.DataConversionException;
import com.linkedin.uif.writer.converter.DataConverter;

/**
 * An implementation of {@link DataWriter} that writes directly
 * to HDFS in Avro format.
 *
 * @param <S> type of source data record representation
 *
 * @author ynli
 */
class AvroHdfsDataWriter<S> implements DataWriter<S, GenericRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(AvroHdfsDataWriter.class);

    private final FileSystem fs;
    private final Path stagingFile;
    private final Path outputFile;
    private final DataConverter<S, GenericRecord> dataConverter;
    private final DataFileWriter<GenericRecord> writer;

    // Number of records successfully written
    private final AtomicLong count = new AtomicLong(0);

    public AvroHdfsDataWriter(URI uri, String stagingDir, String outputDir,
            String fileName, int bufferSize, DataConverter<S,
            GenericRecord> dataConverter, Schema schema) throws IOException {

        Configuration conf = new Configuration();
        this.fs = FileSystem.get(uri, conf);
        this.stagingFile = new Path(stagingDir, fileName);
        this.outputFile = new Path(outputDir, fileName);
        this.dataConverter = dataConverter;
        this.writer = createDatumWriter(schema, this.stagingFile, bufferSize);
    }

    @Override
    public void write(S sourceRecord) throws IOException {
        Preconditions.checkNotNull(sourceRecord);

        try {
            this.writer.append(this.dataConverter.convert(sourceRecord));
        } catch (DataConversionException e) {
            LOG.error("Failed to convert source data record: " + sourceRecord);
            throw new IOException(e);
        }

        // Only increment when write is successful
        this.count.incrementAndGet();
    }

    @Override
    public void close() throws IOException {
        this.writer.flush();
        this.writer.close();
    }

    @Override
    public void commit() throws IOException {
        LOG.info(String.format("Moving data from %s to %s", this.stagingFile, this.outputFile));
        if (this.fs.exists(this.outputFile)) {
            throw new IOException(String.format("File %s already exists", this.outputFile));
        }
        this.fs.rename(this.stagingFile, this.outputFile);
    }

    @Override
    public void cleanup() throws IOException {
        // Delete the staging file
        if (this.fs.exists(this.stagingFile)) {
            this.fs.delete(this.stagingFile, false);
        }
    }

    @Override
    public long recordsWritten() {
        return this.count.get();
    }

    @Override
    public long bytesWritten() throws IOException {
        if (!this.fs.exists(this.outputFile)) {
            return 0;
        }

        return this.fs.getFileStatus(this.outputFile).getLen();
    }

    /**
     * Create a new {@link DataFileWriter} for writing Avro records.
     *
     * @param schema Avro schema
     * @param avroFile Avro file to write to
     * @throws IOException
     */
    private DataFileWriter<GenericRecord> createDatumWriter(Schema schema,
            Path avroFile, int bufferSize) throws IOException {

        if (this.fs.exists(avroFile)) {
            throw new IOException(String.format("File %s already exists", avroFile));
        }

        FSDataOutputStream outputStream = this.fs.create(avroFile, true, bufferSize);
        return new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>())
                .create(schema, outputStream);
    }
}

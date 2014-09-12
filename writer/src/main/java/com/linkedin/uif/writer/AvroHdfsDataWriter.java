package com.linkedin.uif.writer;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.State;
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

    // Whether the writer has already been closed or not
    private volatile boolean closed = false;

    public enum CodecType {
        DEFLATE,
        SNAPPY
    }

    public AvroHdfsDataWriter(State properties, String relFilePath, String fileName,
                              DataConverter<S, GenericRecord> dataConverter, Schema schema)
            throws IOException {

        String uri = properties.getProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI);
        String stagingDir = properties.getProp(ConfigurationKeys.WRITER_STAGING_DIR,
                ConfigurationKeys.DEFAULT_STAGING_DIR) + Path.SEPARATOR + relFilePath;
        String outputDir = properties.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR,
                ConfigurationKeys.DEFAULT_OUTPUT_DIR) + Path.SEPARATOR + relFilePath;
        String codecType = properties.getProp(ConfigurationKeys.WRITER_CODEC_TYPE,
                AvroHdfsDataWriter.CodecType.DEFLATE.name());
        int bufferSize = Integer.parseInt(properties.getProp(
                ConfigurationKeys.WRITER_BUFFER_SIZE, ConfigurationKeys.DEFAULT_BUFFER_SIZE));
        int deflateLevel = Integer.parseInt(properties.getProp(
                ConfigurationKeys.WRITER_DEFLATE_LEVEL, ConfigurationKeys.DEFAULT_DEFLATE_LEVEL));

        Configuration conf = new Configuration();
        // Add all job configuration properties so they are picked up by Hadoop
        for (String key : properties.getPropertyNames()) {
            conf.set(key, properties.getProp(key));
        }
        this.fs = FileSystem.get(URI.create(uri), conf);

        this.stagingFile = new Path(stagingDir, fileName);
        // Deleting the staging file if it already exists, which can happen if the
        // task failed and the staging file didn't get cleaned up for some reason.
        // Deleting the staging file prevents the task retry from being blocked.
        if (this.fs.exists(this.stagingFile)) {
            LOG.warn(String.format(
                    "Task staging file %s already exists, deleting it",
                    this.stagingFile));
            this.fs.delete(this.stagingFile, false);
        }

        this.outputFile = new Path(outputDir, fileName);
        // Create the parent directory of the output file if it does not exist
        if (!this.fs.exists(this.outputFile.getParent())) {
            this.fs.mkdirs(this.outputFile.getParent());
        }

        this.dataConverter = dataConverter;
        this.writer = createDatumWriter(schema, this.stagingFile, bufferSize,
                CodecType.valueOf(codecType), deflateLevel);
    }

    @Override
    public void write(S sourceRecord) throws IOException {
        Preconditions.checkNotNull(sourceRecord);

        try {
            if (this.dataConverter != null) {
                GenericRecord record = this.dataConverter.convert(sourceRecord);
                if (record != null) {
                    this.writer.append(record);
                }
            } else {
                this.writer.append((GenericRecord) sourceRecord);
            }
        } catch (DataConversionException e) {
            LOG.error("Failed to convert and write source data record: " + sourceRecord);
            throw new IOException(e);
        }

        // Only increment when write is successful
        this.count.incrementAndGet();
    }

    @Override
    public void close() throws IOException {
        if (this.closed) {
            return;
        }

        this.writer.flush();
        this.writer.close();
        this.closed = true;
    }

    @Override
    public void commit() throws IOException {
        if (!this.fs.exists(this.stagingFile)) {
            throw new IOException(String.format("File %s does not exist", this.stagingFile));
        }

        LOG.info(String.format("Moving data from %s to %s", this.stagingFile, this.outputFile));
        // For the same reason as deleting the staging file if it already exists, deleting
        // the output file if it already exists prevents task retry from being blocked.
        if (this.fs.exists(this.outputFile)) {
            LOG.warn(String.format("Task output file %s already exists", this.outputFile));
            this.fs.delete(this.outputFile, false);
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
     * @param bufferSize Buffer size
     * @param codecType Compression codec type
     * @param deflateLevel Deflate level
     * @throws IOException
     */
    private DataFileWriter<GenericRecord> createDatumWriter(Schema schema,
            Path avroFile, int bufferSize, CodecType codecType, int deflateLevel)
            throws IOException {

        if (this.fs.exists(avroFile)) {
            throw new IOException(String.format("File %s already exists", avroFile));
        }
        
        FSDataOutputStream outputStream = this.fs.create(avroFile, true, bufferSize);
        DataFileWriter<GenericRecord> writer = new DataFileWriter<GenericRecord>(
                new GenericDatumWriter<GenericRecord>());
        
        // Set compression type
        switch(codecType) {
          case DEFLATE:
              writer.setCodec(CodecFactory.deflateCodec(deflateLevel));
              break;
          case SNAPPY:
              writer.setCodec(CodecFactory.snappyCodec());
              break;
          default:
              writer.setCodec(CodecFactory.deflateCodec(deflateLevel));
              break;
        }
        
        // Open the file and return the DataFileWriter
        return writer.create(schema, outputStream);
    }
}

package com.linkedin.uif.writer.hdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.linkedin.uif.writer.DataWriter;
import com.linkedin.uif.writer.converter.DataConversionException;
import com.linkedin.uif.writer.converter.DataConverter;

/**
 * An implementation of {@link DataWriter} that writes directly
 * to HDFS in Avro format.
 */
public class HdfsDataWriter<T> implements DataWriter<T> {

    private static final Log LOG = LogFactory.getLog(HdfsDataWriter.class);

    private final FileSystem fs;
    private final Path stagingFile;
    private final Path outputFile;
    private final DataConverter<T> dataConverter;
    private final DataFileWriter<GenericRecord> writer;

    private long count = 0;

    public HdfsDataWriter(URI uri, String stagingDir, String outputDir,
            String fileName, int bufferSize, DataConverter<T> dataConverter,
            Schema schema) throws IOException {

        this.fs = FileSystem.get(uri, new Configuration());
        this.stagingFile = new Path(stagingDir, fileName);
        this.outputFile = new Path(outputDir, fileName);
        this.dataConverter = dataConverter;
        this.writer = createDatumWriter(schema, this.stagingFile, bufferSize);
    }

    @Override
    public void write(T sourceRecord) throws IOException {
        try {
            this.writer.append(this.dataConverter.convert(sourceRecord));
        } catch (DataConversionException e) {
            LOG.error("Failed to convert source data record: " + sourceRecord);
            throw new IOException(e);
        }
    // only increment when write is successful
    this.count++;
    }

    @Override
    public void commit() throws IOException {
        this.fs.rename(this.stagingFile, this.outputFile);
    }

    @Override
    public long recordsWritten() {
        return this.count;
    }

    @Override
    public void close() throws IOException {
        this.writer.flush();
        this.writer.close();
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

        FSDataOutputStream outputStream = this.fs.create(
                avroFile, true, bufferSize);
        return new DataFileWriter<GenericRecord>(
                new GenericDatumWriter<GenericRecord>())
                .create(schema, outputStream);
    }
}

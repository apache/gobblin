package com.linkedin.uif.example.extractor;

import java.io.IOException;
import java.net.URI;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.Extractor;

/**
 * An example {@link Extractor} for testing and demonstration purposes.
 */
public class ExampleExtractor implements Extractor<String, String> {

    private static final Logger log = LoggerFactory.getLogger(ExampleExtractor.class);

    private static final String SOURCE_FILE_KEY = "source.file";

    // Test Avro Schema
    private static final String AVRO_SCHEMA =
            "{\"namespace\": \"example.avro\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"User\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"name\", \"type\": \"string\"},\n" +
            "     {\"name\": \"favorite_number\",  \"type\": \"int\"},\n" +
            "     {\"name\": \"favorite_color\", \"type\": \"string\"}\n" +
            " ]\n" +
            "}";

    private static final int TOTAL_RECORDS = 1000;

    private DataFileReader<GenericRecord> dataFileReader;

    public ExampleExtractor(WorkUnitState workUnitState) {
        Schema schema = new Schema.Parser().parse(AVRO_SCHEMA);
        Path sourceFile = new Path(workUnitState.getWorkunit().getProp(SOURCE_FILE_KEY));
        
        log.info("Reading from source file " + sourceFile);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        
        try {
            URI uri = URI.create(workUnitState.getProp(ConfigurationKeys.FS_URI_KEY,
                    ConfigurationKeys.LOCAL_FS_URI));
            FileSystem fs = FileSystem.get(uri, new Configuration());
            fs.makeQualified(sourceFile);
            this.dataFileReader = new DataFileReader<GenericRecord>(
                                  new FsInput(sourceFile,
                                  new Configuration()), datumReader);
        } catch (IOException ioe) {
            log.error("Failed to read the source file " + sourceFile, ioe);
        }
    }

    @Override
    public String getSchema() {
        return AVRO_SCHEMA;
    }

    @Override
    public String readRecord() {
        if (this.dataFileReader == null) {
            return null;
        }

        if (this.dataFileReader.hasNext()) {
            return this.dataFileReader.next().toString();
        }
        return null;
    }

    @Override
    public void close() {
        try {
            this.dataFileReader.close();
        } catch (IOException ioe) {
            log.error("Error while closing avro file reader", ioe);
        }
    }

    @Override
    public long getExpectedRecordCount() {
        return TOTAL_RECORDS;
    }

    @Override
    public long getHighWatermark() {
      return 0;
    }
}
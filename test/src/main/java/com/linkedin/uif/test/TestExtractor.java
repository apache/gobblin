package com.linkedin.uif.test;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.extractor.Extractor;

/**
 * An implementation of {@link Extractor} for integration test.
 *
 * @author ynli
 */
public class TestExtractor extends Extractor<String, String> {

    private static final Log LOG = LogFactory.getLog(TestExtractor.class);

    private static final String SOURCE_FILE_KEY = "source.file";

    // Test Avro schema
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

    public TestExtractor(WorkUnitState workUnitState) {
        super(workUnitState);
        Schema schema = new Schema.Parser().parse(AVRO_SCHEMA);
        File sourceFile = new File(
                workUnitState.getWorkunit().getProp(SOURCE_FILE_KEY));
        LOG.info("Reading from source file " + sourceFile);
        DatumReader<GenericRecord> datumReader =
                new GenericDatumReader<GenericRecord>(schema);
        try {
            this.dataFileReader = new DataFileReader<GenericRecord>(
                    sourceFile, datumReader);
        } catch (IOException ioe) {
            // Ignored
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
            // ignored
        }
    }

    @Override
    public long getExpectedRecordCount() {
        return TOTAL_RECORDS;
    }
}

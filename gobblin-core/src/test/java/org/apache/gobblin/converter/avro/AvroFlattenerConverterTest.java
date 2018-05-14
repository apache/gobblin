package org.apache.gobblin.converter.avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.Test;

import org.junit.Assert;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.util.AvroUtils;

import static org.testng.Assert.*;

public class AvroFlattenerConverterTest {

  @Test
  public void testConvertRecord()
      throws IOException {
    Schema inputSchema = new Schema.Parser().parse(getClass().getResourceAsStream("/converter/nested.avsc"));
    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(inputSchema);
    WorkUnitState workUnitState = new WorkUnitState();

    File tmp = File.createTempFile(this.getClass().getSimpleName(), null);
    FileUtils.copyInputStreamToFile(getClass().getResourceAsStream("/converter/nested.avro"), tmp);
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(tmp, datumReader);
    GenericRecord inputRecord = dataFileReader.next();

    AvroFlattenerConverter converter = new AvroFlattenerConverter();
    Schema outputSchema = null;
    try {
      outputSchema = converter.convertSchema(inputSchema, workUnitState);
    } catch (SchemaConversionException e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertNotNull(outputSchema.getField("address__street_number"));

    GenericRecord outputRecord = null;
    try {
      outputRecord = converter.convertRecord(outputSchema, inputRecord, workUnitState).iterator().next();
    } catch (DataConversionException e) {
      Assert.fail(e.getMessage());
    }
    Object expected = AvroUtils.getFieldValue(inputRecord, "address.street_number").get();
    Assert.assertEquals(expected, outputRecord.get("address__street_number"));

  }
}
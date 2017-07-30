package gobblin.converter.avro;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.Test;

import junit.framework.Assert;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.util.AvroUtils;


@Test
public class FlattenNestedKeyConverterTest {
  /**
   * Test schema and record conversion
   *  1. A successful schema and record conversion
   *  2. Another successful conversion by reusing the converter
   *  3. An expected failed conversion by reusing the converter
   */
  public void testConversion()
      throws IOException {
    String key = FlattenNestedKeyConverter.class.getSimpleName() + "." + FlattenNestedKeyConverter.FIELDS_TO_FLATTEN;
    Properties props = new Properties();
    props.put(key, "name,address.street_number");
    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.addAll(props);

    Schema inputSchema = new Schema.Parser().parse(getClass().getResourceAsStream("/converter/nested.avsc"));
    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(inputSchema);

    File tmp = File.createTempFile(this.getClass().getSimpleName(), null);
    FileUtils.copyInputStreamToFile(getClass().getResourceAsStream("/converter/nested.avro"), tmp);
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(tmp, datumReader);
    GenericRecord inputRecord = dataFileReader.next();

    FlattenNestedKeyConverter converter = new FlattenNestedKeyConverter();
    Schema outputSchema = null;
    try {
      outputSchema = converter.convertSchema(inputSchema, workUnitState);
    } catch (SchemaConversionException e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertTrue(outputSchema.getFields().size() == inputSchema.getFields().size() + 1);
    Assert.assertTrue(outputSchema.getField("addressStreet_number") != null);

    GenericRecord outputRecord = null;
    try {
      outputRecord = converter.convertRecord(outputSchema, inputRecord, workUnitState).iterator().next();
    } catch (DataConversionException e) {
      Assert.fail(e.getMessage());
    }
    Object expected = AvroUtils.getFieldValue(outputRecord, "address.street_number").get();
    Assert.assertTrue(outputRecord.get("addressStreet_number") == expected);

    // Reuse the converter to do another successful conversion
    props.put(key, "name,address.city");
    workUnitState.addAll(props);
    try {
      outputSchema = converter.convertSchema(inputSchema, workUnitState);
    } catch (SchemaConversionException e) {
      Assert.fail(e.getMessage());
    }
    Assert.assertTrue(outputSchema.getFields().size() == inputSchema.getFields().size() + 1);
    Assert.assertTrue(outputSchema.getField("addressCity") != null);
    try {
      outputRecord = converter.convertRecord(outputSchema, inputRecord, workUnitState).iterator().next();
    } catch (DataConversionException e) {
      Assert.fail(e.getMessage());
    }
    expected = AvroUtils.getFieldValue(outputRecord, "address.city").get();
    Assert.assertTrue(outputRecord.get("addressCity") == expected);

    // Reuse the converter to do a failed conversion
    props.put(key, "name,address.anInvalidField");
    workUnitState.addAll(props);
    boolean hasAnException = false;
    try {
      converter.convertSchema(inputSchema, workUnitState);
    } catch (SchemaConversionException e) {
      hasAnException = true;
    }
    Assert.assertTrue(hasAnException);
  }
}

package gobblin.converter.avro;

import org.apache.avro.generic.GenericRecord;
import org.codehaus.jackson.JsonNode;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.test.TestUtils;
import gobblin.type.RecordWithMetadata;


public class AvroToJsonRecordWithMetadataConverterTest {
  private AvroToJsonRecordWithMetadataConverter converter;
  private WorkUnitState state;
  private GenericRecord sampleRecord;

  @BeforeTest
  public void setUp() throws SchemaConversionException {
    sampleRecord = TestUtils.generateRandomAvroRecord();
    state = new WorkUnitState();

    converter = new AvroToJsonRecordWithMetadataConverter();
    converter.convertSchema(sampleRecord.getSchema(), state);
  }

  @Test
  public void testRecord() throws DataConversionException {
    Iterable<RecordWithMetadata<JsonNode>> records = converter.convertRecord(null, sampleRecord, state);

    RecordWithMetadata<JsonNode> node = records.iterator().next();
    Assert.assertEquals(node.getMetadata().getGlobalMetadata().getContentType(), "test.name+json");
    Assert.assertEquals(node.getRecord().get("field1").getTextValue(), sampleRecord.get("field1").toString());
  }
}

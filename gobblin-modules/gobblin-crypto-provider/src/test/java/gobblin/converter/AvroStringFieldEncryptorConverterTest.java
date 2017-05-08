package gobblin.converter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;

import gobblin.configuration.WorkUnitState;
import gobblin.test.TestUtils;
import gobblin.test.crypto.InsecureShiftCodec;


public class AvroStringFieldEncryptorConverterTest {
  @Test
  public void testConversion()
      throws DataConversionException, IOException, SchemaConversionException {
    AvroStringFieldEncryptorConverter converter = new AvroStringFieldEncryptorConverter();
    WorkUnitState wuState = new WorkUnitState();

    wuState.getJobState().setProp("converter.fieldsToEncrypt", "field1");
    wuState.getJobState().setProp("converter.encrypt.algorithm", "insecure_shift");

    converter.init(wuState);
    GenericRecord inputRecord = TestUtils.generateRandomAvroRecord();

    Schema inputSchema = inputRecord.getSchema();
    Schema outputSchema = converter.convertSchema(inputSchema, wuState);

    String fieldValue = (String) inputRecord.get("field1");

    Iterable<GenericRecord> recordIt = converter.convertRecord(outputSchema, inputRecord, wuState);
    GenericRecord record = recordIt.iterator().next();

    Assert.assertEquals(outputSchema, inputSchema);
    String encryptedValue = (String) record.get("field1");

    InsecureShiftCodec codec = new InsecureShiftCodec(Maps.<String, Object>newHashMap());
    InputStream in = codec.decodeInputStream(new ByteArrayInputStream(encryptedValue.getBytes(StandardCharsets.UTF_8)));
    byte[] decryptedValue = new byte[in.available()];
    in.read(decryptedValue);

    Assert.assertEquals(new String(decryptedValue, StandardCharsets.UTF_8), fieldValue);
  }
}

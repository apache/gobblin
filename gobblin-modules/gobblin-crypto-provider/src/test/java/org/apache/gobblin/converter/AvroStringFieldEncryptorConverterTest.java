package gobblin.converter;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;

import gobblin.configuration.WorkUnitState;
import gobblin.test.TestUtils;
import gobblin.test.crypto.InsecureShiftCodec;


public class AvroStringFieldEncryptorConverterTest {
  @Test
  public void testNestedConversion()
      throws DataConversionException, IOException, SchemaConversionException {
    AvroStringFieldEncryptorConverter converter = new AvroStringFieldEncryptorConverter();
    WorkUnitState wuState = new WorkUnitState();

    wuState.getJobState().setProp("converter.fieldsToEncrypt", "nestedRecords.*.fieldToEncrypt");
    wuState.getJobState().setProp("converter.encrypt.algorithm", "insecure_shift");

    converter.init(wuState);
    GenericRecord inputRecord =
        getRecordFromFile(getClass().getClassLoader().getResource("record_with_arrays.avro").getPath());

    Schema inputSchema = inputRecord.getSchema();
    Schema outputSchema = converter.convertSchema(inputSchema, wuState);

    List<String> origValues = new ArrayList<>();
    for (Object o : (List) inputRecord.get("nestedRecords")) {
      GenericRecord r = (GenericRecord) o;
      origValues.add(r.get("fieldToEncrypt").toString());
    }

    Iterable<GenericRecord> recordIt = converter.convertRecord(outputSchema, inputRecord, wuState);
    GenericRecord record = recordIt.iterator().next();

    Assert.assertEquals(outputSchema, inputSchema);

    List<String> decryptedValues = new ArrayList<>();
    for (Object o : (List) record.get("nestedRecords")) {
      GenericRecord r = (GenericRecord) o;
      String encryptedValue = r.get("fieldToEncrypt").toString();

      InsecureShiftCodec codec = new InsecureShiftCodec(Maps.<String, Object>newHashMap());
      InputStream in =
          codec.decodeInputStream(new ByteArrayInputStream(encryptedValue.getBytes(StandardCharsets.UTF_8)));
      byte[] decryptedValue = new byte[in.available()];
      in.read(decryptedValue);
      decryptedValues.add(new String(decryptedValue, StandardCharsets.UTF_8));
    }

    Assert.assertEquals(decryptedValues, origValues);
  }

  private GenericRecord getRecordFromFile(String path) throws IOException {
    DatumReader<GenericRecord> reader = new GenericDatumReader<>();
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(new File(path), reader);
    while (dataFileReader.hasNext()) {
      return dataFileReader.next();
    }

    return null;
  }

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

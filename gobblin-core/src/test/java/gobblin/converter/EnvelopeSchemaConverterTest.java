package gobblin.converter;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.EnvelopeSchemaConverter;
import gobblin.converter.filter.AvroProjectionConverter;
import gobblin.converter.filter.AvroSchemaFieldRemover;
import gobblin.metrics.kafka.KafkaAvroSchemaRegistry;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class EnvelopeSchemaConverterTest {

  private final String SCHEMA_KEY = "testKey";

  private GenericRecord mockInputRecord = mock(GenericRecord.class);
  private GenericRecord mockOutputRecord = mock(GenericRecord.class);
  private Schema mockSchema = mock(Schema.class);

  class KafkaAvroSchemaRegistryForTest extends KafkaAvroSchemaRegistry {
    public KafkaAvroSchemaRegistryForTest(Properties props) {
      super(props);
    }

    @Override
    public Schema getSchemaByKey(String key) {
      if (key.equals(SCHEMA_KEY)) {
        return mockSchema;
      } else {
        return null;
      }
    }
  }

  class EnvelopeSchemaConverterForTest extends EnvelopeSchemaConverter {
    @Override
    public EnvelopeSchemaConverterForTest init(WorkUnitState workUnit) {
      if (workUnit.contains(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY)) {
        String removeFieldsPropName = workUnit.getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY) + AvroProjectionConverter.REMOVE_FIELDS;
        if (workUnit.contains(removeFieldsPropName)) {
          this.fieldRemover = Optional.of(new AvroSchemaFieldRemover(workUnit.getProp(removeFieldsPropName)));
        } else {
          this.fieldRemover = Optional.absent();
        }
      }
      this.registry = new KafkaAvroSchemaRegistryForTest(workUnit.getProperties());
      this.decoderFactory = DecoderFactory.get();
      this.readers = CacheBuilder.newBuilder().build(new CacheLoader<Schema, GenericDatumReader<GenericRecord>>() {
        @Override
        public GenericDatumReader<GenericRecord> load(final Schema key) throws Exception {
          return new GenericDatumReader<>(key);
        }
      });
      return this;
    }

    @Override
    public byte[] getPayload(GenericRecord inputRecord, String payloadFieldName) {
      return null;
    }

    @Override
    public GenericRecord deserializePayload(byte[] payload, Schema payloadSchema) {
      Assert.assertEquals(payloadSchema, mockSchema);
      return mockOutputRecord;
    }
  }

  @Test
  public void convertRecordTest() throws Exception {

    when(mockInputRecord.get("payloadSchemaId")).thenReturn(SCHEMA_KEY);
    when(mockOutputRecord.getSchema()).thenReturn(mockSchema);

    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, "testEvent");
    workUnitState.setProp("kafka.schema.registry.url", "testUrl");

    EnvelopeSchemaConverterForTest converter = new EnvelopeSchemaConverterForTest();
    converter.init(workUnitState);
    GenericRecord output = converter.convertRecord(null, mockInputRecord, workUnitState).iterator().next();
    Assert.assertEquals(output, mockOutputRecord);
  }
}

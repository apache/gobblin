package gobblin.converter;

import gobblin.metrics.kafka.KafkaAvroSchemaRegistry;
import gobblin.metrics.kafka.KafkaSchemaRegistry;
import gobblin.metrics.kafka.KafkaSchemaRegistryFactory;
import java.util.Properties;
import org.apache.avro.Schema;

/**
 * Override some methods of {@link KafkaAvroSchemaRegistry} for use in {@link EnvelopeSchemaConverterTest}
 */
public class KafkaAvroSchemaRegistryForTest extends KafkaAvroSchemaRegistry {
  public static class Factory implements KafkaSchemaRegistryFactory {
    public Factory() {}

    public KafkaSchemaRegistry create(Properties props) {
      return new KafkaAvroSchemaRegistryForTest(props);
    }
  }

  public KafkaAvroSchemaRegistryForTest(Properties props) {
    super(props);
  }

  @Override
  public Schema getSchemaByKey(String key) {
    if (key.equals(EnvelopeSchemaConverterTest.SCHEMA_KEY)) {
      return EnvelopeSchemaConverterTest.mockSchema;
    } else {
      return null;
    }
  }
}
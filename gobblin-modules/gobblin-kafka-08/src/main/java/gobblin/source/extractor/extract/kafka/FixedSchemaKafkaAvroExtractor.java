package gobblin.source.extractor.extract.kafka;

import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import com.google.common.base.Preconditions;

import gobblin.annotation.Alias;
import gobblin.configuration.WorkUnitState;


/**
 * Extract avro records from a Kafka topic using a fixed schema provided by {@link #STATIC_SCHEMA_ROOT_KEY}.
 */
@Alias(value = "AVRO_FIXED_SCHEMA")
public class FixedSchemaKafkaAvroExtractor extends KafkaAvroExtractor<Void> {

  public static final String STATIC_SCHEMA_ROOT_KEY = "gobblin.source.kafka.fixedSchema";

  public FixedSchemaKafkaAvroExtractor(WorkUnitState state) {
    super(state);
  }

  @Override
  protected Schema getLatestSchemaByTopic(String topic) {
    String key = STATIC_SCHEMA_ROOT_KEY + "." + topic;
    Preconditions.checkArgument(this.workUnitState.contains(key),
        String.format("Could not find schema for topic %s. Looking for key %s.", topic, key));
    return new Schema.Parser().parse(this.workUnitState.getProp(key));
  }

  @Override
  protected Schema getRecordSchema(byte[] payload) {
    if (!this.schema.isPresent()) {
      throw new RuntimeException("Schema is not preset. This is an error in the code.");
    }
    return this.schema.get();
  }

  @Override
  protected Decoder getDecoder(byte[] payload) {
    return DecoderFactory.get().binaryDecoder(payload, null);
  }
}

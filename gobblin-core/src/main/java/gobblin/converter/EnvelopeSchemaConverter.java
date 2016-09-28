package gobblin.converter;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.filter.AvroProjectionConverter;
import gobblin.converter.filter.AvroSchemaFieldRemover;
import gobblin.metrics.kafka.KafkaAvroSchemaRegistry;
import gobblin.metrics.kafka.SchemaRegistryException;
import gobblin.util.AvroUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import javax.xml.bind.DatatypeConverter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

/**
 * A converter for opening GoldenGate envelope schema.
 */
public class EnvelopeSchemaConverter extends Converter<Schema, String, GenericRecord, GenericRecord> {

  public static final String PAYLOAD_SCHEMA_ID = "envelope.schemaIdField";
  public static final String PAYLOAD = "envelope.payloadField";

  protected Optional<AvroSchemaFieldRemover> fieldRemover;
  protected KafkaAvroSchemaRegistry registry;
  protected DecoderFactory decoderFactory;
  protected LoadingCache<Schema, GenericDatumReader<GenericRecord>> readers;

  /**
   * To remove certain fields from the Avro schema or records of a topic/table, set property
   * {topic/table name}.remove.fields={comma-separated, fully qualified field names} in workUnit.
   *
   * E.g., PageViewEvent.remove.fields=header.memberId,mobileHeader.osVersion
   */
  @Override
  public EnvelopeSchemaConverter init(WorkUnitState workUnit) {
    if (workUnit.contains(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY)) {
      String removeFieldsPropName = workUnit.getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY) + AvroProjectionConverter.REMOVE_FIELDS;
      if (workUnit.contains(removeFieldsPropName)) {
        this.fieldRemover = Optional.of(new AvroSchemaFieldRemover(workUnit.getProp(removeFieldsPropName)));
      } else {
        this.fieldRemover = Optional.absent();
      }
    }
    this.registry = new KafkaAvroSchemaRegistry(workUnit.getProperties());
    this.decoderFactory = DecoderFactory.get();
    this.readers = CacheBuilder.newBuilder().build(new CacheLoader<Schema, GenericDatumReader<GenericRecord>>() {
      @Override
      public GenericDatumReader<GenericRecord> load(final Schema key) throws Exception {
        return new GenericDatumReader<>(key);
      }
    });
    return this;
  }

  /**
   * Do nothing, actual schema must be obtained from records.
   */
  @Override
  public String convertSchema(Schema inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return EnvelopeSchemaConverter.class.getName();
  }

  /**
   * Get actual schema from registry and deserialize payload using it.
   */
  @Override
  public Iterable<GenericRecord> convertRecord(String outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    try {
      String schemaIdField = workUnit.contains(PAYLOAD_SCHEMA_ID) ? workUnit.getProp(PAYLOAD_SCHEMA_ID) : "payloadSchemaId";
      String payloadField = workUnit.contains(PAYLOAD) ? workUnit.getProp(PAYLOAD) : "payload";
      String schemaKey = String.valueOf(inputRecord.get(schemaIdField));
      Schema payloadSchema = this.registry.getSchemaByKey(schemaKey);
      byte[] payload = getPayload(inputRecord, payloadField);
      GenericRecord outputRecord = deserializePayload(payload, payloadSchema);
      if (this.fieldRemover.isPresent()) {
        payloadSchema = this.fieldRemover.get().removeFields(payloadSchema);
      }
      return new SingleRecordIterable<>(AvroUtils.convertRecordSchema(outputRecord, payloadSchema));
    } catch (IOException | SchemaRegistryException | ExecutionException e) {
      throw new DataConversionException(e);
    }
  }

  /**
   * Get payload field from GenericRecord and convert to byte array
   */
  public byte[] getPayload(GenericRecord inputRecord, String payloadFieldName) {
    ByteBuffer bb = (ByteBuffer) inputRecord.get(payloadFieldName);
    String hexString = new String(bb.array());
    return DatatypeConverter.parseHexBinary(hexString);
  }

  /**
   * Deserialize payload using payload schema
   */
  public GenericRecord deserializePayload(byte[] payload, Schema payloadSchema) throws IOException, ExecutionException {
    Decoder decoder = this.decoderFactory.binaryDecoder(payload, null);
    GenericDatumReader<GenericRecord> reader = this.readers.get(payloadSchema);
    return reader.read(null, decoder);
  }
}

package gobblin.converter;

import com.google.common.base.Optional;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.filter.AvroProjectionConverter;
import gobblin.converter.filter.AvroSchemaFieldRemover;
import gobblin.metrics.kafka.KafkaAvroSchemaRegistry;
import gobblin.metrics.kafka.SchemaRegistryException;
import gobblin.util.AvroUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import javax.xml.bind.DatatypeConverter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * A converter for opening GoldenGate envelope schema.
 */
public class EnvelopeSchemaConverter extends AvroToAvroConverterBase {

  public static final String PAYLOAD_SCHEMA_ID = "envelope.schema.id.field";
  public static final String PAYLOAD = "envelope.payload.field";

  private Optional<AvroSchemaFieldRemover> fieldRemover;
  private KafkaAvroSchemaRegistry registry;

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
    return this;
  }

  /**
   * Do nothing, actual schema must be obtained from records.
   */
  @Override
  public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return inputSchema;
  }

  /**
   * Get actual schema from registry and deserialize payload using it.
   */
  @Override
  public Iterable<GenericRecord> convertRecord(Schema outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    try {
      if (registry == null) {
        registry = new KafkaAvroSchemaRegistry(workUnit.getProperties());
      }
      String schemaKey = String.valueOf(inputRecord.get(workUnit.getProp(PAYLOAD_SCHEMA_ID)));
      Schema payloadSchema = registry.getSchemaByKey(schemaKey);
      byte[] payload = getPayload(inputRecord, workUnit.getProp(PAYLOAD));
      GenericRecord outputRecord = AvroUtils.slowDeserializeGenericRecord(payload, payloadSchema);
      if (this.fieldRemover.isPresent()) {
        payloadSchema = this.fieldRemover.get().removeFields(payloadSchema);
      }
      return new SingleRecordIterable<>(AvroUtils.convertRecordSchema(outputRecord, payloadSchema));
    } catch (IOException | SchemaRegistryException e) {
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
}

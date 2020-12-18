package org.apache.gobblin.converter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import org.apache.gobblin.kafka.client.DecodeableKafkaRecord;


/**
 * A {@link org.apache.gobblin.stream.ControlMessageInjector} that detects changes in the latest schema and notifies downstream constructs by
 * injecting a {@link org.apache.gobblin.stream.MetadataUpdateControlMessage}.
 * Also supports multi-dataset schema changes.
 */
public class GenericRecordBasedKafkaSchemaChangeInjector extends KafkaSchemaChangeInjector<Schema> {
  @Override
  protected Schema getSchemaIdentifier(DecodeableKafkaRecord consumerRecord) {
    GenericRecord genericRecord = (GenericRecord) consumerRecord.getValue();
    return genericRecord.getSchema();
  }
}
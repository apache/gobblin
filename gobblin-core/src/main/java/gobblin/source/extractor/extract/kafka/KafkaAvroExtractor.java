package gobblin.source.extractor.extract.kafka;

import gobblin.configuration.WorkUnitState;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public class KafkaAvroExtractor extends KafkaExtractor<Schema, GenericRecord> {

  public KafkaAvroExtractor(WorkUnitState state) {
    super(state);
    // TODO Auto-generated constructor stub
  }

}

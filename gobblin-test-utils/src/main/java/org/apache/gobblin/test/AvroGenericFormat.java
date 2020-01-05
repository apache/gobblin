package org.apache.gobblin.test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.test.generator.Field;
import org.apache.gobblin.test.generator.ValueConvertingGenerator;
import org.apache.gobblin.test.generator.ValueGenerator;
import org.apache.gobblin.test.type.Type;


@Slf4j
public class AvroGenericFormat implements Format<Schema, GenericRecord>, ValueGenerator<GenericRecord> {



  private static final Map<Type, Schema.Type> logicalToAvroTypes = new HashMap<>();

  private final ArrayList<ValueGenerator> valueGenerators = new ArrayList<>();
  private Schema schema = null;

  @Override
  public Type getLogicalType() {
    return Type.Struct;
  }

  @Override
  public GenericRecord get() {
    return generateRandomRecord();
  }

  static {
    logicalToAvroTypes.put(Type.Integer, Schema.Type.INT);
    logicalToAvroTypes.put(Type.Long, Schema.Type.LONG);
    logicalToAvroTypes.put(Type.Bytes, Schema.Type.BYTES);
    logicalToAvroTypes.put(Type.String, Schema.Type.STRING);
    logicalToAvroTypes.put(Type.Boolean, Schema.Type.BOOLEAN);
  }

  @Override
  public Schema generateRandomSchema(List<Field> requiredFields) {
    ArrayList<Field> allFields = FormatUtils.generateRandomFields();
    ArrayList<Schema.Field> fields = new ArrayList<Schema.Field>();
    valueGenerators.clear();

    allFields.addAll(requiredFields);

    for (Field requiredField: allFields) {
      log.info("Field: {}", requiredField);
      String fieldName = requiredField.getName();
      Type fieldType = requiredField.getType();
      Schema fieldSchema = Schema.create(logicalToAvroTypes.get(fieldType));
      String docString = "required field:" + TestUtils.generateRandomAlphaString(5);
      switch (requiredField.getOptionality()) {
        case OPTIONAL: {
          List<Schema> unionSchema = new ArrayList<Schema>(2);
          unionSchema.add(Schema.create(Schema.Type.NULL));
          unionSchema.add(fieldSchema);
          Schema unionWithNullSchema = Schema.createUnion(unionSchema);
          fieldSchema = unionWithNullSchema;
        }
      }
      fields.add(new Schema.Field(fieldName, fieldSchema, docString, null));
      final Type type = requiredField.getType();
      switch (type) {
        case Bytes: {
          // Avro needs ByteBuffers for byte arrays. The default value generator will generate a byte[]
          valueGenerators.add(new ValueConvertingGenerator<byte[], ByteBuffer>(
              requiredField.getValueGenerator(), ByteBuffer::wrap));
          break;
        }
        case String: {
          // Avro prefers Utf8 over java String classes
          valueGenerators.add(new ValueConvertingGenerator<String, Utf8>(
              requiredField.getValueGenerator(), Utf8::new));
          break;
        }
        default: valueGenerators.add(requiredField.getValueGenerator());
      }
    }
    Schema schema = Schema.createRecord("name", "Random Test Schema", "test",false);
    schema.setFields(fields);
    this.schema = schema;
    return schema;
  }

  @Override
  public GenericRecord generateRandomRecord() {
    Preconditions.checkArgument(this.schema !=null,
        "Generate Random Record called without calling generate random schema");
    GenericData.Record record = new GenericData.Record(this.schema);
    int index = 0;
    for (Schema.Field field: this.schema.getFields()) {
      record.put(field.pos(), valueGenerators.get(index).get());
      index++;
    }
    return record;
  }
}

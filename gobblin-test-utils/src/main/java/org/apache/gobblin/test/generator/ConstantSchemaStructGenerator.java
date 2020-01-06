package org.apache.gobblin.test.generator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.test.ConfigurableValueGenerator;
import org.apache.gobblin.test.TestUtils;
import org.apache.gobblin.test.type.Type;


@ConfigurableValueGenerator(name="constant", targetTypes = Type.Struct, configuration = ConstantSchemaGeneratorConfig.class)
@Slf4j
public class ConstantSchemaStructGenerator implements StructValueGenerator<GenericRecord> {

  private static final Map<Type, Schema.Type> logicalToAvroTypes = new HashMap<>();
  private final List<Field> logicalFields;

  @Override
  public List<Field> getStructSchema() {
    return this.logicalFields;
  }

  static {
    logicalToAvroTypes.put(Type.Integer, Schema.Type.INT);
    logicalToAvroTypes.put(Type.Long, Schema.Type.LONG);
    logicalToAvroTypes.put(Type.Bytes, Schema.Type.BYTES);
    logicalToAvroTypes.put(Type.String, Schema.Type.STRING);
    logicalToAvroTypes.put(Type.Boolean, Schema.Type.BOOLEAN);
  }

  private final List<ValueGenerator> valueGenerators;
  @Getter
  private final Schema avroSchema;


  List<Field> resolveFields(List<FieldConfig> fieldConfigs) {
    List<Field> fields = new ArrayList<>();
    for (FieldConfig fieldConfig : fieldConfigs) {
      String valueGeneratorName = fieldConfig.getValueGen();
      Config valueGeneratorConfig = fieldConfig.getValueGenConfig();
      if (valueGeneratorConfig == null) {
        valueGeneratorConfig = ConfigFactory.empty();
      }
      if (valueGeneratorName == null) {
        valueGeneratorName = "random";
      }
      // Get the registered value generator for this name and target type
      ValueGenerator valueGenerator = null;
      try {
        valueGenerator = ValueGeneratorFactory.getInstance().getValueGenerator(valueGeneratorName, fieldConfig);
      } catch (Exception e) {
        log.error("Could not create value generator with name {}", valueGeneratorName);
        throw new RuntimeException(e);
      }
      List<Field> innerFields = null;
      if (fieldConfig.type == Type.Struct) {
        StructValueGenerator structValueGenerator = (StructValueGenerator) valueGenerator;
        innerFields = structValueGenerator.getStructSchema();
      }
      Field field = new Field(fieldConfig.getName(),
          fieldConfig.getType(),
          fieldConfig.getOptional(),
          valueGenerator,
          innerFields,
          fieldConfig.getSymbols());
      fields.add(field);
    }
      return fields;
  }

  ValueGenerator getValueGenerator(Field logicalField, final Schema.Field avroField) {
    // TODO: wrap optional
    final Type type = logicalField.getType();
    switch (type) {
      case Bytes: {
        // Avro needs ByteBuffers for byte arrays. The default value generator will generate a byte[]
        return (new ValueConvertingGenerator<byte[], ByteBuffer>(logicalField.getValueGenerator(), ByteBuffer::wrap));
      }
      case String: {
        // Avro prefers Utf8 over java String classes
        return (new ValueConvertingGenerator<String, Utf8>(logicalField.getValueGenerator(), Utf8::new));
      }
      case Enum: {
        // Convert Enum String to Avro Enum Class
        return (new ValueConvertingGenerator<String, GenericEnumSymbol>
        (logicalField.getValueGenerator(),
        stringVal -> new GenericData.EnumSymbol(avroField.schema(), stringVal)
        ));
      }
      default:
        return logicalField.getValueGenerator();
    }
  }

  public ConstantSchemaStructGenerator(ConstantSchemaGeneratorConfig config) {
    Preconditions.checkArgument(config != null);
    Preconditions.checkArgument(config.getFieldConfig() != null);
    this.logicalFields = resolveFields(config.getFieldConfig().getFields());

    // Set up Avro Schema
    List<Schema.Field> fields = logicalFields.stream()
    .map(this::getAvroField)
    .collect(Collectors.toList());
    Schema schema = Schema.createRecord(config.getFieldConfig().getName(),
        "Random Test Schema",
        "test",
        false);
    schema.setFields(fields);

    switch (config.getFieldConfig().getOptional()) {
      case OPTIONAL: {
        List<Schema> unionSchema = new ArrayList<Schema>(2);
        unionSchema.add(Schema.create(Schema.Type.NULL));
        unionSchema.add(schema);
        Schema unionWithNullSchema = Schema.createUnion(unionSchema);
        this.avroSchema = unionWithNullSchema;
        break;
      }
      case REQUIRED: {
        this.avroSchema = schema;
        break;
      }
      default: throw new RuntimeException();
    }


    this.valueGenerators = IntStream.range(0, logicalFields.size())
        .mapToObj(i -> getValueGenerator(logicalFields.get(i), fields.get(i)))
        .collect(Collectors.toList());
  }

  private Schema.Field getAvroField(Field requiredField) {
    log.info("Field: {}", requiredField);
    String fieldName = requiredField.getName();
    Type fieldType = requiredField.getType();
    Schema fieldSchema;
    String docString = "required field:" + TestUtils.generateRandomAlphaString(5);
    // primitive
    switch (fieldType) {
      case Struct: {
        // create an embedded constant / random schema struct generator
        fieldSchema = Schema.createRecord(requiredField.getName(), "Random Test Schema", "test", false);
        fieldSchema.setFields(requiredField.getFields().stream().map(this::getAvroField).collect(Collectors.toList()));
      }
      break;
      case Enum: {
        fieldSchema = Schema.createEnum(requiredField.getName(), "Random doc string", "test", requiredField.getSymbols());
      }
      break;
      default: {
        if (logicalToAvroTypes.containsKey(fieldType)) {
          fieldSchema = Schema.create(logicalToAvroTypes.get(fieldType));
        } else {
          throw new RuntimeException("Not implemented");
        }
      }
    }
    switch (requiredField.getOptionality()) {
      case OPTIONAL: {
        List<Schema> unionSchema = new ArrayList<Schema>(2);
        unionSchema.add(Schema.create(Schema.Type.NULL));
        unionSchema.add(fieldSchema);
        Schema unionWithNullSchema = Schema.createUnion(unionSchema);
        fieldSchema = unionWithNullSchema;
      }
    }
    return new Schema.Field(fieldName, fieldSchema, docString, null);

  }

  @Override
  public Type getLogicalType() {
    return Type.Struct;
  }

  @Override
  public GenericRecord get() {
    Schema recordSchema = this.avroSchema;
    if (this.avroSchema.getType() == Schema.Type.UNION) {
      // pull out the second one
      recordSchema = this.avroSchema.getTypes().get(1);
    }
    GenericData.Record record = new GenericData.Record(recordSchema);
    int index = 0;
    for (Schema.Field field: recordSchema.getFields()) {
      record.put(field.pos(), valueGenerators.get(index).get());
      index++;
    }
    return record;
  }
}

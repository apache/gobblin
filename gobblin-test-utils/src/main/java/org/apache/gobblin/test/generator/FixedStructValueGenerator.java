package org.apache.gobblin.test.generator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.test.generator.config.FixedSchemaGeneratorConfig;
import org.apache.gobblin.test.generator.config.FieldConfig;
import org.apache.gobblin.test.type.Type;

@Slf4j
public abstract class FixedStructValueGenerator<T> implements StructValueGenerator<T> {
  protected final List<FieldConfig> logicalFields;
  protected final List<FieldConfigWithValueGenerator> logicalFieldGenerators;

  public static List<FieldConfigWithValueGenerator> resolveFields(List<FieldConfig> fieldConfigs, ValueGeneratorFactory factory) {
    List<FieldConfigWithValueGenerator> fields = new ArrayList<>();
    for (FieldConfig fieldConfig : fieldConfigs) {
      FieldConfigWithValueGenerator fieldConfigWithValueGenerator = null;
      String valueGeneratorName = fieldConfig.getValueGen();
      if (valueGeneratorName == null) {
        valueGeneratorName = "random";
      }
      // Get the registered value generator for this name and target type
      ValueGenerator valueGenerator = null;
      try {
        valueGenerator = factory.getValueGenerator(valueGeneratorName, fieldConfig);
        Preconditions.checkState(valueGenerator != null,
            "Failed to find a value generator with name "
                + valueGeneratorName + " fieldConfig: " + fieldConfig.toString());

      } catch (Exception e) {
        log.error("Could not create value generator with name {}", valueGeneratorName);
        throw new RuntimeException(e);
      }
      List<FieldConfig> innerFields = null;
      if (fieldConfig.getType() == Type.Struct) {
        StructValueGenerator structValueGenerator = (StructValueGenerator) valueGenerator;
        innerFields = structValueGenerator.getStructSchema();
        FieldConfig structFieldConfig = fieldConfig.toBuilder()
            .clearFields()
            .fields(
                // extract just the field configs from the returned field-config, valuegen pairs
                innerFields)
            .build();
        fieldConfigWithValueGenerator = new FieldConfigWithValueGenerator(structFieldConfig, structValueGenerator);
      } else {
        fieldConfigWithValueGenerator =
            new FieldConfigWithValueGenerator(fieldConfig, valueGenerator);
      }
      fields.add(fieldConfigWithValueGenerator);
    }
    return fields;
  }

  public FixedStructValueGenerator(FixedSchemaGeneratorConfig config, ValueGeneratorFactory valueGeneratorFactory) {
    Preconditions.checkArgument(config != null);
    Preconditions.checkArgument(config.getFieldConfig() != null);
    this.logicalFieldGenerators = resolveFields(config.getFieldConfig().getFields(), valueGeneratorFactory);
    this.logicalFields = this.logicalFieldGenerators.stream().map(x -> x.getFieldConfig()).collect(Collectors.toList());
  }

  @Override
  public List<FieldConfig> getStructSchema() {
    return this.logicalFields;
  }

  @Override
  public Type getLogicalType() {
    return Type.Struct;
  }

}

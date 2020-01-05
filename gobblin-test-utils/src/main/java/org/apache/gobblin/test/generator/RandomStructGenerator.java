package org.apache.gobblin.test.generator;

import java.util.ArrayList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.test.ConfigurableValueGenerator;
import org.apache.gobblin.test.Optionality;
import org.apache.gobblin.test.TestUtils;
import org.apache.gobblin.test.type.Type;

@Slf4j
@ConfigurableValueGenerator(name="random", targetTypes = Type.Struct, configuration = RandomStructGeneratorConfig.class)
public class RandomStructGenerator extends ConstantSchemaStructGenerator {

  private static ConstantSchemaGeneratorConfig getFixedConfig(RandomStructGeneratorConfig config) {
    int maxFields = config.getMaxFields();
    FieldConfig topLevelFieldConfig = config.getFieldConfig();
    List<FieldConfig> providedFields = new ArrayList<>();
    if (topLevelFieldConfig == null || topLevelFieldConfig.getFields() == null) {
    } else {
      providedFields.addAll(topLevelFieldConfig.getFields());
    }
    int numRandomFields = maxFields - providedFields.size();
    log.info("Max fields = {}, Provided fields = {}, num Random fields = {}", maxFields, providedFields, numRandomFields);
    for (int i = 0; i < numRandomFields; ++i) {
      FieldConfig fieldConfig = new FieldConfig();
      fieldConfig.setName("f" + i + "_" + TestUtils.generateRandomAlphaString(4));
      Type type = config.getTypes()[TestUtils.generateRandomInteger(config.getTypes().length)];
      while ((type == Type.Struct) && (config.getMaxDepth() <= topLevelFieldConfig.getDepth())) {
        log.warn("Reached maximum depth, flipping coin again");
        type = config.getTypes()[TestUtils.generateRandomInteger(config.getTypes().length)];
      }
      fieldConfig.setType(type);
      if (type.equals(Type.Struct)) {
        fieldConfig.setTypeName("struct" + TestUtils.generateRandomAlphaString(3));
      }
      fieldConfig.setOptional(TestUtils.generateRandomBool()? Optionality.REQUIRED:Optionality.OPTIONAL);
      fieldConfig.setValueGen("random");
      fieldConfig.setDoc("Random field");
      fieldConfig.setDepth(topLevelFieldConfig.getDepth()+1);
      providedFields.add(fieldConfig);
    }
    ConstantSchemaGeneratorConfig fixedConfig = new ConstantSchemaGeneratorConfig();
    log.info("{}",config.getFieldConfig());
    FieldConfig fieldConfig = config.getFieldConfig().toBuilder()
        .fields(providedFields)
        .build();
    /**
    fieldConfig.setName(config.getFieldConfig().getName());
    fieldConfig.setType(config.getFieldConfig().getType());
    fieldConfig.setOptional(config.getFieldConfig().getOptional());
    fieldConfig.setFields(providedFields);
     **/

    fixedConfig.setFieldConfig(fieldConfig);
    return fixedConfig;
  }

  public RandomStructGenerator(RandomStructGeneratorConfig config) {
    super(getFixedConfig(config));
  }

}

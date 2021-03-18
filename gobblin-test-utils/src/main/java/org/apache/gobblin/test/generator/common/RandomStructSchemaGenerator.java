package org.apache.gobblin.test.generator.common;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.test.Optionality;
import org.apache.gobblin.test.TestUtils;
import org.apache.gobblin.test.generator.config.FixedSchemaGeneratorConfig;
import org.apache.gobblin.test.generator.config.FieldConfig;
import org.apache.gobblin.test.generator.config.RandomStructGeneratorConfig;
import org.apache.gobblin.test.type.Type;


@Slf4j
public class RandomStructSchemaGenerator {

  /**
   * Generate a random schema given an optional input schema
   * @param config
   * @return
   */
  public static FixedSchemaGeneratorConfig generateSchemaConfig(RandomStructGeneratorConfig config) {
    int maxFields = config.getMaxFields();
    FieldConfig topLevelFieldConfig = config.getFieldConfig();
    int providedFieldsSize = ((topLevelFieldConfig == null || topLevelFieldConfig.getFields() == null))
        ?0:topLevelFieldConfig.getFields().size();
    int numRandomFields = maxFields - providedFieldsSize;
    log.info("Max fields = {}, Provided fields = {}, num Random fields = {}", maxFields, providedFieldsSize, numRandomFields);
    FieldConfig.FieldConfigBuilder<?, ?> fieldConfigBuilder = config.getFieldConfig().toBuilder();
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
      // add this fieldConfig to the existing builder
      fieldConfigBuilder.field(fieldConfig);
    }
    FixedSchemaGeneratorConfig fixedConfig = new FixedSchemaGeneratorConfig();
    log.info("{}",config.getFieldConfig());
    fixedConfig.setFieldConfig(fieldConfigBuilder.build());
    return fixedConfig;
  }
}

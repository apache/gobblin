package org.apache.gobblin.test.generator.json;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.test.ConfigurableValueGenerator;
import org.apache.gobblin.test.InMemoryFormat;
import org.apache.gobblin.test.generator.common.RandomStructSchemaGenerator;
import org.apache.gobblin.test.generator.config.RandomStructGeneratorConfig;
import org.apache.gobblin.test.type.Type;


@ConfigurableValueGenerator(
    name="random",
    targetTypes = Type.Struct,
    targetFormat = InMemoryFormat.JSON,
    configClass = RandomStructGeneratorConfig.class)
@Slf4j
public class JsonRandomSchemaStructGenerator extends JsonFixedSchemaStructGenerator {
  public JsonRandomSchemaStructGenerator(RandomStructGeneratorConfig config) {
    super(RandomStructSchemaGenerator.generateSchemaConfig(config));
  }
}

package org.apache.gobblin.test.generator.avro;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.test.ConfigurableValueGenerator;
import org.apache.gobblin.test.InMemoryFormat;
import org.apache.gobblin.test.generator.common.RandomStructSchemaGenerator;
import org.apache.gobblin.test.generator.config.RandomStructGeneratorConfig;
import org.apache.gobblin.test.type.Type;

@Slf4j
@ConfigurableValueGenerator(
    name="random",
    targetTypes = Type.Struct,
    targetFormat = InMemoryFormat.AVRO_GENERIC,
    configClass = RandomStructGeneratorConfig.class)
public class AvroRandomStructGenerator extends AvroFixedSchemaStructGenerator {

  public AvroRandomStructGenerator(RandomStructGeneratorConfig config) {
    super(RandomStructSchemaGenerator.generateSchemaConfig(config));
  }

}

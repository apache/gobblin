package org.apache.gobblin.test.generator;

import java.util.function.BiFunction;
import java.util.stream.Stream;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.test.ConfigurableValueGenerator;
import org.apache.gobblin.test.InMemoryFormat;
import org.apache.gobblin.test.config.MagicConfiguringFactory;
import org.apache.gobblin.test.generator.config.FieldConfig;
import org.apache.gobblin.test.generator.config.FieldConfigAware;
import org.apache.gobblin.test.type.Type;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


/**
 * A single format Value Generator factory that holds value generators for different logical types of data.
 */
@Slf4j
public class ValueGeneratorFactory extends MagicConfiguringFactory<ValueGenerator, ConfigurableValueGenerator> {

  private static BiFunction<String, Type, String> uuidGenerator =
    (name, type) -> name.toLowerCase() + ":" + type.toString().toLowerCase();

  protected ValueGeneratorFactory(InMemoryFormat inMemoryFormat) {
    super("ValueGenerator",
        "org.apache.gobblin",
        ValueGenerator.class,
        ConfigurableValueGenerator.class,
        namedValueGenerator ->
            Stream.of(namedValueGenerator)
            .filter(annotation -> annotation.targetFormat().equals(inMemoryFormat))
            .flatMap(annotation -> Stream.of(annotation.targetTypes()))
            .map(type -> uuidGenerator.apply(namedValueGenerator.name(), type)),
        namedValueGenerator -> namedValueGenerator.configClass()
        );
  }

  public ValueGenerator getValueGenerator(String alias, FieldConfig config)
      throws ValueGeneratorNotFoundException {
    log.info("Get value generator for {} with config {}", alias, config);
    String uuid = uuidGenerator.apply(alias, config.getType());

    Class valueGeneratorClass = super.getInstanceClass(uuid);
    if (valueGeneratorClass == null) {
      throw new ValueGeneratorNotFoundException("Could not find value generator with alias " + alias + " uuid: " + uuid);
    }
    Class configClass = super.getConfigClass(valueGeneratorClass);

    Config valueGenConfig = config.getValueGenConfig();
    if (valueGenConfig == null) {
      valueGenConfig = ConfigFactory.empty();
    }

    Object configObject;
    try {
      configObject = ConfigBeanFactory.create(valueGenConfig, configClass);
    } catch (ConfigException ce) {
      log.error("Unable to instantiate class {} from valueGenConfig {}", configClass, valueGenConfig);
      throw new RuntimeException(ce);
    }


    if (configObject instanceof FieldConfigAware) {
      ((FieldConfigAware) configObject).setFieldConfig(config);
    }

    log.info("Invoking constructor for class {} with config class {}", valueGeneratorClass, configObject.getClass());
    ValueGenerator v =
        (ValueGenerator) GobblinConstructorUtils.invokeConstructor(valueGeneratorClass, valueGeneratorClass.getName(), configObject);
    return v;
  }
}

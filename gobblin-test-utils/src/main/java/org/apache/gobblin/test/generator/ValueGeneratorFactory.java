package org.apache.gobblin.test.generator;

import java.util.function.BiFunction;
import java.util.stream.Stream;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.test.ConfigurableValueGenerator;
import org.apache.gobblin.test.config.MagicFactory;
import org.apache.gobblin.test.type.Type;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;

@Slf4j
public class ValueGeneratorFactory extends MagicFactory<ValueGenerator, ConfigurableValueGenerator> {

  private static BiFunction<String, Type, String> uuidGenerator =
      (name, type) -> name.toLowerCase() + ":" + type.toString().toLowerCase();

  private ValueGeneratorFactory() {
    super("ValueGenerator", "org.apache.gobblin", ValueGenerator.class, ConfigurableValueGenerator.class,
        namedValueGenerator -> Stream.of(namedValueGenerator.targetTypes())
            .map(type -> uuidGenerator.apply(namedValueGenerator.name(), type)),
        namedValueGenerator -> namedValueGenerator.configuration()
        );
  }

  private static ValueGeneratorFactory INSTANCE = new ValueGeneratorFactory();

  public static ValueGeneratorFactory getInstance() {
    return INSTANCE;
  }

  public ValueGenerator getValueGenerator(String name, FieldConfig config)
      throws ClassNotFoundException {
    log.info("Get value generator for {} with config {}", name, config);
    String uuid = uuidGenerator.apply(name, config.getType());

    Class valueGeneratorClass = super.getInstanceClass(uuid);
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

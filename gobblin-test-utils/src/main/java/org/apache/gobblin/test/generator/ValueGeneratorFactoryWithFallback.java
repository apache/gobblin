package org.apache.gobblin.test.generator;

import java.util.function.BiFunction;
import java.util.stream.Stream;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.test.InMemoryFormat;
import org.apache.gobblin.test.config.AnnotationRegisteredFactory;
import org.apache.gobblin.test.generator.config.FieldConfig;
import org.apache.gobblin.test.type.Type;

@Slf4j
public class ValueGeneratorFactoryWithFallback extends ValueGeneratorFactory {

  private final InMemoryFormat inMemoryFormat;
  private final ValueGeneratorFactory fallbackValueGeneratorFactory;
  private final AnnotationRegisteredFactory<ValueConvertingFunction, ValueConverter> valueConverterRegistry;

  private final BiFunction<InMemoryFormat, Type, String> uuidGenerator =
      (format, type) -> format.name() + ":" + type.name();

  public ValueGeneratorFactoryWithFallback(InMemoryFormat inMemoryFormat, ValueGeneratorFactory valueGeneratorFactory) {
    super(inMemoryFormat);
    this.inMemoryFormat = inMemoryFormat;
    this.fallbackValueGeneratorFactory = valueGeneratorFactory;
    /**
    Function<ValueConverter, Stream<String>> uuidStreamGenerator =
      valueConverter ->
          Stream.of(valueConverter)
          .flatMap(annotation -> Stream.of(annotation.logicalTypes()))
          .map(type -> uuidGenerator.apply(valueConverter.inMemoryType(), type));
    **/


    this.valueConverterRegistry = new AnnotationRegisteredFactory<>(
        inMemoryFormat.name() + "-valueConverterRegistry",
        "org.apache.gobblin",
        ValueConvertingFunction.class,
        ValueConverter.class,
        valueConverter ->
            Stream.of(valueConverter)
                .flatMap(annotation -> Stream.of(annotation.logicalTypes()))
                .map(type -> uuidGenerator.apply(valueConverter.inMemoryType(), type))
    );
  }

  @Override
  public ValueGenerator getValueGenerator(String alias, FieldConfig config)
      throws ValueGeneratorNotFoundException {
    ValueGenerator valueGenerator;
    try {
      valueGenerator = super.getValueGenerator(alias, config);
    } catch (ValueGeneratorNotFoundException vgnfe) {
      // use fallback generator
      valueGenerator = fallbackValueGeneratorFactory.getValueGenerator(alias, config);
    }
      // wrap with a value converter if any are registered
      String uuid = uuidGenerator.apply(inMemoryFormat, config.getType());
      if (this.valueConverterRegistry.canProduce(uuid)) {
        ValueConvertingFunction valueConverter = this.valueConverterRegistry.getThing(uuid).withConfig(config);
        valueGenerator = new ValueConvertingGenerator(valueGenerator, valueConverter);
      }
    return valueGenerator;
  }
}

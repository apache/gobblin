package org.apache.gobblin.test.generator;

import java.util.function.Function;

import org.apache.gobblin.test.generator.config.FieldConfig;


public interface ValueConvertingFunction<I, O> extends Function<I, O> {

  default ValueConvertingFunction withConfig(FieldConfig config) { return this;}

}

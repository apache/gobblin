package org.apache.gobblin.test.generator;

import lombok.AllArgsConstructor;
import lombok.Getter;

import org.apache.gobblin.test.generator.config.FieldConfig;

@AllArgsConstructor @Getter
public class FieldConfigWithValueGenerator {

  FieldConfig fieldConfig;
  ValueGenerator valueGenerator;

}

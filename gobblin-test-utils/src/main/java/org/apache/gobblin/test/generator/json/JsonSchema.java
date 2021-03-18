package org.apache.gobblin.test.generator.json;

import lombok.AllArgsConstructor;
import lombok.Getter;

import org.apache.gobblin.test.generator.config.FieldConfig;


@AllArgsConstructor
public class JsonSchema {
  @Getter
  FieldConfig fieldConfig;
}

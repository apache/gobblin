package org.apache.gobblin.test.generator.config;

import com.typesafe.config.Optional;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;


@Getter @Setter @NoArgsConstructor @AllArgsConstructor
@SuperBuilder
public class FieldConfigComposer implements FieldConfigAware {
  @Optional
  FieldConfig fieldConfig;
}

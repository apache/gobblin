package org.apache.gobblin.test.generator;

import com.typesafe.config.Optional;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@Getter @Setter @NoArgsConstructor
public class FieldConfigComposer implements FieldConfigAware {
  @Optional
  FieldConfig fieldConfig;
}

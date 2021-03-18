package org.apache.gobblin.test.generator.config;

import com.typesafe.config.Optional;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@Getter
@Setter
@NoArgsConstructor
public class RandomValueGeneratorConfig extends FieldConfigComposer {
  @Optional
  long seed = 42;
}

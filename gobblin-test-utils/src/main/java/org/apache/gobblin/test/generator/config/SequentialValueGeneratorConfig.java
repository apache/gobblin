package org.apache.gobblin.test.generator.config;

import com.typesafe.config.Optional;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@NoArgsConstructor
@Getter
@Setter
public class SequentialValueGeneratorConfig<T> extends FieldConfigComposer {
  @Optional
  T initValue = null;
}

package org.apache.gobblin.test.generator.config;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@NoArgsConstructor @Getter @Setter
public class ConstantValueGeneratorConfig<T> extends FieldConfigComposer {
  T value;
}

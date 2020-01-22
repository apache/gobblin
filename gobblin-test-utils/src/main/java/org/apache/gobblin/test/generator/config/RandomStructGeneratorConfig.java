package org.apache.gobblin.test.generator.config;

import com.typesafe.config.Optional;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import org.apache.gobblin.test.type.Type;


@NoArgsConstructor
@Getter
@Setter
public class RandomStructGeneratorConfig extends FieldConfigComposer {
  @Optional
  private int maxFields = 5;
  @Optional
  private int maxDepth = 10;
  @Optional
  private Type[] types = {Type.Integer, Type.String, Type.Boolean, Type.Struct};
  @Optional
  private FieldConfig fieldConfig;
}

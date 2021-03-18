package org.apache.gobblin.test.generator.config;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import org.apache.gobblin.test.type.Type;


@Getter
@Setter
@NoArgsConstructor
public class MultiTypeValueGeneratorConfig {
  Type logicalType;
}

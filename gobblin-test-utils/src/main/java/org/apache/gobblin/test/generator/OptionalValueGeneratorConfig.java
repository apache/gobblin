package org.apache.gobblin.test.generator;

import com.typesafe.config.Optional;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@NoArgsConstructor
@Getter
@Setter
public class OptionalValueGeneratorConfig {
  private static final int DEFAULT_NULL_PERCENTAGE = 5;
  @Optional
  private int nullPercentage = DEFAULT_NULL_PERCENTAGE;
}

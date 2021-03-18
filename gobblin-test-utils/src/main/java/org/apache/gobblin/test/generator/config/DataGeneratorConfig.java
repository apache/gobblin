package org.apache.gobblin.test.generator.config;

import com.typesafe.config.Optional;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import org.apache.gobblin.test.InMemoryFormat;


@NoArgsConstructor @AllArgsConstructor
@Getter @Setter @ToString @SuperBuilder
public class DataGeneratorConfig extends FieldConfigComposer {
  @Optional @Builder.Default
  InMemoryFormat inMemoryFormat = InMemoryFormat.AVRO_GENERIC;
  @Optional @Builder.Default
  long totalRecords = 200;
  @Optional @Builder.Default
  Boolean keepLocalCopy = false;
}

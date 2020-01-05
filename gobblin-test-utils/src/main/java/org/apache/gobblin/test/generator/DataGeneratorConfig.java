package org.apache.gobblin.test.generator;

import com.typesafe.config.Config;
import com.typesafe.config.Optional;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import org.apache.gobblin.test.Formats;


@NoArgsConstructor
@Getter
@Setter
public class DataGeneratorConfig {
  String schemaGen;
  Config schemaGenConfig;
  @Optional
  FieldConfig schema;
  @Optional
  Formats.InMemoryFormat inMemoryFormat = Formats.InMemoryFormat.AVRO_GENERIC;
  @Optional
  int totalRecords = 200;
}

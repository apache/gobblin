package org.apache.gobblin.test.generator;

import java.util.List;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@NoArgsConstructor
@Getter
@Setter
public class StructConfig extends FieldConfig {
  List<FieldConfig> fields;
}

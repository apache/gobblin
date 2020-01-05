package org.apache.gobblin.test.generator;

import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.Optional;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import org.apache.gobblin.test.Optionality;
import org.apache.gobblin.test.type.Type;


@NoArgsConstructor
@Getter
@Setter
@AllArgsConstructor
@ToString
@Builder(toBuilder = true)
public class FieldConfig {
  @Optional
  String name;
  Type type;
  @Optional
  String typeName;
  @Optional
  Optionality optional = Optionality.REQUIRED;
  @Optional
  String doc;
  @Optional
  String valueGen = "random";
  @Optional
  Config valueGenConfig;
  @Optional
  List<FieldConfig> fields = Collections.emptyList();
  @Optional
  List<String> symbols = Collections.EMPTY_LIST;
  @Optional
  int depth = 0;

  public void validate() {
    switch (type) {
      case Enum: {
        Preconditions.checkArgument(this.symbols !=null && this.symbols.size() > 0);
        break;
      }
    }
  }
}

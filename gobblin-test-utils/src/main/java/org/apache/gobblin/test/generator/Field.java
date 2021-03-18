package org.apache.gobblin.test.generator;

import java.util.List;

import com.google.common.base.Preconditions;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;

import org.apache.gobblin.test.Optionality;
import org.apache.gobblin.test.type.Type;


@Data
public class Field {
  @Getter
  private final String name;
  @Getter
  private final Type type;
  @Getter
  private final Optionality optionality;
  private final ValueGenerator valueGenerator;

  private final List<Field> fields;

  private final List<String> symbols; // for enumeration fields

  @Builder
  public Field(String name,
      Type type,
      Optionality optionality,
      ValueGenerator valueGenerator,
      List<Field> fields,
      List<String> symbols) {
    this.name = name;
    this.type = type;
    this.optionality = optionality == null? Optionality.OPTIONAL: optionality;
    Preconditions.checkArgument(valueGenerator.getLogicalType().equals(this.type),
        "ValueGenerator does not generate the same type");
    this.valueGenerator = valueGenerator;
    if (this.type == Type.Struct) {
      Preconditions.checkArgument(fields != null, "Fields list must be provided by struct");
    } else {
      Preconditions.checkArgument(fields == null || fields.size() == 0, "Fields list must not be "
          + "provided by type " + type.name());
    }
    this.fields = fields;
    this.symbols = symbols;
  }
}

package org.apache.gobblin.util.reflection;

import lombok.Getter;
import lombok.Setter;


public class EnclosedClass {
  public EnclosedClass(int value) {
    this.value = value;
  }

  @Setter
  @Getter
  int value;
}

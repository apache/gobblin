package org.apache.gobblin.test;

import java.util.List;

import org.apache.gobblin.test.generator.Field;


public interface Format<S, D> {

  S generateRandomSchema(List<Field> fields);

  D generateRandomRecord();

}

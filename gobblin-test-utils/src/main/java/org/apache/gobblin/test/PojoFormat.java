package org.apache.gobblin.test;

import java.util.List;

import org.apache.gobblin.test.generator.Field;
import org.apache.gobblin.test.generator.ValueGenerator;


public class PojoFormat implements Format<Object, TestRecord> {

//  private long sequence;
  private ValueGenerator<Integer> partitionValueGenerator;
  private ValueGenerator<Integer> sequenceValueGenerator;

  @Override
  public Object generateRandomSchema(List<Field> fields) {
    for (Field f : fields) {
      if (f.getName().equals("partition")) {
        partitionValueGenerator = f.getValueGenerator();
      }
      if (f.getName().equals("sequence")) {
        sequenceValueGenerator = f.getValueGenerator();
      }
    }
    return TestRecord.class;
  }

  @Override
  public TestRecord generateRandomRecord() {
    return new TestRecord(partitionValueGenerator.get(),
        sequenceValueGenerator.get(), "");
  }
}

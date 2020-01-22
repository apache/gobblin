package org.apache.gobblin.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.gobblin.test.generator.Field;
import org.apache.gobblin.test.generator.java.JavaValueGenerator;
import org.apache.gobblin.test.generator.common.OptionalValueGenerator;
import org.apache.gobblin.test.generator.ValueGenerator;
import org.apache.gobblin.test.generator.java.StringValueGenerator;
import org.apache.gobblin.test.type.Type;

@Deprecated
public class FormatUtils {
  static Random random = new Random();

  private static final Map<Type, ValueGenerator> logicalToRandomValueGenerators = new HashMap<>();

  private static final Type[] types = {
      Type.Integer,
      Type.Long,
      Type.Bytes,
      Type.String,
      Type.Boolean
  };

  static {

    logicalToRandomValueGenerators.put(Type.Integer, new IntValueGenerator(GeneratorAlgo.RANDOM, 0));
    logicalToRandomValueGenerators.put(Type.Long, new JavaValueGenerator<Long>() {
      @Override
      public Type getLogicalType() {
        return Type.Long;
      }

      @Override
      public Long get() {
        return TestUtils.generateRandomLong();
      }
    });
    logicalToRandomValueGenerators.put(Type.String, new StringValueGenerator(GeneratorAlgo.RANDOM, ""));
    logicalToRandomValueGenerators.put(Type.Bytes, new JavaValueGenerator<byte[]>() {
      @Override
      public Type getLogicalType() {
        return Type.Bytes;
      }

      @Override
      public byte[] get() {
        return TestUtils.generateRandomBytes(10);
      }
    });
    logicalToRandomValueGenerators.put(Type.Boolean, new JavaValueGenerator() {
      @Override
      public Type getLogicalType() {
        return Type.Boolean;
      }

      @Override
      public Boolean get() {
        return TestUtils.generateRandomBool();
      }
    });
  }



    public static ArrayList<Field> generateRandomFields() {
    return generateRandomFields(types);
  }

    public static ArrayList<Field> generateRandomFields(Type[] typeSet) {
      int numFields = random.nextInt(20);
      ArrayList<Field> randomFields = new ArrayList<>(numFields);
      for (int i = 0; i < numFields; i++) {
        Type logicalFieldType = typeSet[random.nextInt(typeSet.length)];
        String fieldName = "f" + i;

        Optionality optionality = random.nextBoolean()? Optionality.REQUIRED: Optionality.OPTIONAL;
        Field.FieldBuilder fieldBuilder = Field.builder()
            .name(fieldName)
            .type(logicalFieldType)
            .optionality(optionality);

        Field field;
        switch (optionality) {
          case OPTIONAL: {
            field = fieldBuilder
                .valueGenerator(new OptionalValueGenerator(
                logicalToRandomValueGenerators.get(logicalFieldType))
                )
                .build();
            break;
          }
          case REQUIRED: {
            field = fieldBuilder
                .valueGenerator(logicalToRandomValueGenerators.get(logicalFieldType))
                .build();
            break;
          }
          default: throw new RuntimeException("Unexpected optionality value");
        }
        randomFields.add(field);
      }
      return randomFields;
    }
}

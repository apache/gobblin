package org.apache.gobblin.test.generator.java;

import java.util.HashMap;
import java.util.Map;

import org.apache.gobblin.test.type.Type;


public class JavaTypes {

  public static Object getPhysicalType(Type logicalType) {
    return logicalToPhysicalTypes.get(logicalType);
  }

  private static final Map<Type, Object> logicalToPhysicalTypes = new HashMap<>();
  static {
    logicalToPhysicalTypes.put(Type.Integer, Integer.class);
    logicalToPhysicalTypes.put(Type.Long, Long.class);
    logicalToPhysicalTypes.put(Type.Boolean, Boolean.class);
    logicalToPhysicalTypes.put(Type.String, String.class);
    logicalToPhysicalTypes.put(Type.Bytes, byte[].class);
    logicalToPhysicalTypes.put(Type.Enum, String.class); // Enums are returned as String values
    //TODO: add more types. add check to validate that all types are represented
  }
}

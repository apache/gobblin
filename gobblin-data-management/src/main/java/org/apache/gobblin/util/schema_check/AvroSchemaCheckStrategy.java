package org.apache.gobblin.util.schema_check;

import java.util.HashSet;
import java.util.Set;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;


/**
 * The strategy to compare Avro schema.
 */
public class AvroSchemaCheckStrategy {
  /**
   * Make sure schema toValidate and expected have matching names and types.
   * @param toValidate The real schema
   * @param expected The expected schema
   * @return
   */
  public static boolean compare(Schema toValidate, Schema expected) {
    if (toValidate.getType() != expected.getType() || !toValidate.getName().equals(expected.getName())) {return false;}
    else {
      switch (toValidate.getType()) {
        case NULL:
        case BOOLEAN:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case BYTES:
        case STRING: {
          return true;
        }
        case ARRAY: {
          return compare(toValidate.getElementType(), expected.getElementType());
        }
        case MAP: {
          return compare(toValidate.getValueType(), expected.getValueType());
        }
        case FIXED: {
          // fixed size and name must match:
          if (toValidate.getFixedSize() != expected.getFixedSize()) {
            return false;
          }
        }
        case ENUM: {
          // expected symbols must contain all toValidate symbols:
          final Set<String> expectedSymbols = new HashSet<String>(expected.getEnumSymbols());
          final Set<String> toValidateSymbols = new HashSet<String>(toValidate.getEnumSymbols());
          if (expectedSymbols.size() != toValidateSymbols.size()) {
            return false;
          }
          if (!expectedSymbols.containsAll(toValidateSymbols)) {
            return false;
          }
        }

        case RECORD: {
          // Check that each field of toValidate schema is in expected schema
          if(toValidate.getFields().size() != expected.getFields().size()) {return false;}
          for (final Schema.Field expectedFiled : expected.getFields()) {
            final Schema.Field toValidateField = toValidate.getField(expectedFiled.name());
            if (toValidateField == null) {
              // expected field does not correspond to any field in the toValidate record schema
              return false;
            } else {
              if (!compare(toValidateField.schema(), expectedFiled.schema())) {
                return false;
              }
            }
          }
          return true;
        }
        case UNION: {
          // Check existing schema contains all the type in toValidate schema
          if (toValidate.getTypes().size() != expected.getTypes().size()) {return false;}
          HashSet<Schema> types = new HashSet<Schema>(expected.getTypes());
          for (Schema toValidateType : toValidate.getTypes()) {
            Schema equalSchema = null;
            for (Schema type : types) {
              if (compare(type, toValidateType)) {
                equalSchema = type;
                break;
              }
            }
            if (equalSchema == null) { return false; }
            types.remove(equalSchema);
          }
          return true;
        }
        default: {
          throw new AvroRuntimeException("Unknown schema type: " + toValidate.getType());
        }
      }
    }
  }
}

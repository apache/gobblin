package com.linkedin.uif.util;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;

public class AvroUtils {

  /**
   * Given a GenericRecord, this method will return the field specified by the path parameter. The path parameter is
   * an ordered array specifying the location of the nested field to retrieve. For example, field1.nestedField1 takes
   * the the value of the field "field1", and retrieves the field "nestedField1" from it.
   * @param record is the record to retrieve the field from
   * @param path is the location of the field
   * @return the value of the field
   */
  public static Object getField(GenericRecord record, String[] path) {
    return extractFieldHelper(record, path, 0);
  }

  /**
   * Helper method that does the actual work for {@link #getField(GenericRecord, String[])}
   * @param data passed from {@link #extractField(Object, String[])}
   * @param fieldPath passed from {@link #extractField(Object, String[])}
   * @param field keeps track of the index used to access the array fieldPath
   * @return the value of the field
   */
  private static Object extractFieldHelper(Object data, String[] fieldPath, int field) {
    if (data == null) {
      return null;
    }
    if ((field + 1) == fieldPath.length) {
      Object result = ((Record) data).get(fieldPath[field]);
      if (result == null) {
        return null;
      } else {
        return result;
      }
    } else {
      return extractFieldHelper(((Record) data).get(fieldPath[field]), fieldPath, ++field);
    }
  }
}

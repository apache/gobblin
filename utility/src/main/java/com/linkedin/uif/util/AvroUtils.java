package com.linkedin.uif.util;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

public class AvroUtils {

  /**
   * Given a GenericRecord, this method will return the field specified by the path parameter. The fieldLocation
   * parameter is an ordered array specifying the location of the nested field to retrieve. For example,
   * field1.nestedField1 takes the the value of the field "field1", and retrieves the field "nestedField1" from it.
   * @param record is the record to retrieve the field from
   * @param fieldLocation is the location of the field
   * @return the value of the field
   */
  public static Optional<Object> getField(GenericRecord record, String fieldLocation) {
    Preconditions.checkNotNull(record);
    Preconditions.checkNotNull(fieldLocation);

    Splitter splitter = Splitter.on(".").omitEmptyStrings().trimResults();
    String[] pathArray = Iterables.toArray(splitter.split(fieldLocation), String.class);

    if (pathArray.length == 0) {
      return Optional.absent();
    }

    return extractFieldHelper(record, pathArray, 0);
  }

  /**
   * Helper method that does the actual work for {@link #getField(GenericRecord, String[])}
   * @param data passed from {@link #extractField(Object, String[])}
   * @param fieldPath passed from {@link #extractField(Object, String[])}
   * @param field keeps track of the index used to access the array fieldPath
   * @return the value of the field
   */
  private static Optional<Object> extractFieldHelper(Object data, String[] fieldPath, int field) {
    if ((field + 1) == fieldPath.length) {
      Object result = ((Record) data).get(fieldPath[field]);
      if (result == null) {
        return Optional.absent();
      } else {
        return Optional.of(result);
      }
    } else {
      return extractFieldHelper(((Record) data).get(fieldPath[field]), fieldPath, ++field);
    }
  }
}

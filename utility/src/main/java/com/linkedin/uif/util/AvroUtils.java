/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.uif.util;

import java.util.List;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * A Utils class for dealing with Avro objects
 */
public class AvroUtils {

  private static final String FIELD_LOCATION_DELIMITER = ".";

  /**
   * Given a GenericRecord, this method will return the field specified by the path parameter. The fieldLocation
   * parameter is an ordered string specifying the location of the nested field to retrieve. For example,
   * field1.nestedField1 takes the the value of the field "field1", and retrieves the field "nestedField1" from it.
   * @param record is the record to retrieve the field from
   * @param fieldLocation is the location of the field
   * @return the value of the field
   */
  public static Optional<Object> getField(GenericRecord record, String fieldLocation) {
    Preconditions.checkNotNull(record);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(fieldLocation));

    Splitter splitter = Splitter.on(FIELD_LOCATION_DELIMITER).omitEmptyStrings().trimResults();
    List<String> pathList = Lists.newArrayList(splitter.split(fieldLocation));

    if (pathList.size() == 0) {
      return Optional.absent();
    }

    return AvroUtils.getFieldHelper(record, pathList, 0);
  }

  /**
   * Helper method that does the actual work for {@link #getField(GenericRecord, String)}
   * @param data passed from {@link #getField(Object, String)}
   * @param pathList passed from {@link #getField(Object, String)}
   * @param field keeps track of the index used to access the list pathList
   * @return the value of the field
   */
  private static Optional<Object> getFieldHelper(Object data, List<String> pathList, int field) {
    if ((field + 1) == pathList.size()) {
      Object result = ((Record) data).get(pathList.get(field));
      if (result == null) {
        return Optional.absent();
      } else {
        return Optional.of(result);
      }
    } else {
      return AvroUtils.getFieldHelper(((Record) data).get(pathList.get(field)), pathList, ++field);
    }
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gobblin.recordaccess;

import java.util.Map;

import gobblin.annotation.Alpha;


/**
 * Interface for an object that can introspect and manipulate a record. Ideal use case
 * is for converter logic; converters can specify the operation to do while the
 * RecordAccessor actually carries it out.
 *
 * In general, nested records should be accessible with a '.' separating record name:
 *  eg given
 *  "foo": {
 *    "bar: 1
 *  }
 *
 *  "foo.bar" should refer to integer 1.
 *
 * This interface will likely grow over time with more accessors/setters as well as
 * schema manipulation (for example: rename field or delete field operations).
 */
@Alpha
public interface RecordAccessor {

  /*
   * Access a particular field of a record.
   *
   * If a record is nested, the "." may be used to access
   * pieces of the nested record.
   *
   * If a record contains an array, ".0", ".1", etc may be used to index into the particular
   * element of the array.
   *
   * In the getMulti() variants, a ".*" may be used to access all elements of an array or map.
   * A map of key -> value is returned.
   *
   * For example, given the following JSON record:
   * {
   *   "nested": {
   *     "key": "val"
   *   },
   *   "nestedArr": [
   *     { "key": "val0" },
   *     { "key": "val1" }
   *   ]
   * }
   *
   * getAsString("nested.key") should return "val0".
   * getAsString("nested.1.key") should return "val1".
   * getAsString("nested.*.key") will throw an exception since the '*' refers to multiple values.
   * getMultiAsString("nested.*.key") should return the map (nestedArr.0.key->val0, nestedArr.1.key->val1).
   *
   * Getters should return null if the field does not exist; may throw
   * IncorrectTypeException if the underlying types do not match. Getters should not
   * try to do any type coercion -- for example, getAsInt for a value that is the string "1"
   * should throw a Sch.
   */
  Map<String, String> getMultiAsString(String fieldName);
  Map<String, Integer> getMultiAsInt(String fieldName);
  Map<String, Long> getMultiAsLong(String fieldName);

  String getAsString(String fieldName);
  Integer getAsInt(String fieldName);
  Long getAsLong(String fieldName);

  /*
   * Set new values for an object. Should throw a FieldDoesNotExistException runtime exception if fieldName
   * is not present in the object's schema or an IncorrectTypeException if the underlying type does not match.
   */
  void set(String fieldName, String value);
  void set(String fieldName, Integer value);
  void set(String fieldName, Long value);
  void setToNull(String fieldName);


}

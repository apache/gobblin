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

package org.apache.gobblin.source.extractor.watermark;

/**
 * An implementation for predicate conditions
 * columnName : name of the column
 * value: value
 * condition: predicate condition using column and value
 * format: column format
 */
public class Predicate {
  public String columnName;
  public long value;
  public String condition;
  public String format;
  public PredicateType type;

  /**
   * Enum which lists the predicate types
   * LWM - low water mark and HWM - high water mark
   */
  public enum PredicateType {
    LWM,
    HWM
  }

  public Predicate(String columnName, long value, String condition, String format, PredicateType type) {
    this.columnName = columnName;
    this.value = value;
    this.condition = condition;
    this.format = format;
    this.type = type;
  }

  public String getColumnName() {
    return this.columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public long getValue() {
    return this.value;
  }

  public void setValue(long value) {
    this.value = value;
  }

  public String getCondition() {
    return this.condition;
  }

  public void setCondition(String condition) {
    this.condition = condition;
  }

  public String getFormat() {
    return this.format;
  }

  public void setFormat(String format) {
    this.format = format;
  }

  public PredicateType getType() {
    return this.type;
  }

  public void setType(PredicateType type) {
    this.type = type;
  }
}

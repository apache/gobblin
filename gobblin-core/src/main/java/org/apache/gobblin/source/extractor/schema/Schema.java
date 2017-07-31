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

package org.apache.gobblin.source.extractor.schema;

import com.google.gson.JsonObject;


/**
 * Schema from extractor
 */
public class Schema {
  private String columnName;
  private JsonObject dataType;
  private boolean isWaterMark;
  private int primaryKey;
  private long length;
  private int precision;
  private int scale;
  private boolean isNullable;
  private String format;
  private String comment;
  private String defaultValue;
  private boolean isUnique;

  public String getColumnName() {
    return this.columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public JsonObject getDataType() {
    return this.dataType;
  }

  public void setDataType(JsonObject dataType) {
    this.dataType = dataType;
  }

  public int getPrimaryKey() {
    return this.primaryKey;
  }

  public void setPrimaryKey(int primaryKey) {
    this.primaryKey = primaryKey;
  }

  public long getLength() {
    return this.length;
  }

  public void setLength(long length) {
    this.length = length;
  }

  public int getPrecision() {
    return this.precision;
  }

  public void setPrecision(int precision) {
    this.precision = precision;
  }

  public int getScale() {
    return this.scale;
  }

  public void setScale(int scale) {
    this.scale = scale;
  }

  public String getFormat() {
    return this.format;
  }

  public void setFormat(String format) {
    this.format = format;
  }

  public String getComment() {
    return this.comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  public String getDefaultValue() {
    return this.defaultValue;
  }

  public void setDefaultValue(String defaultValue) {
    this.defaultValue = defaultValue;
  }

  public boolean isWaterMark() {
    return this.isWaterMark;
  }

  public void setWaterMark(boolean isWaterMark) {
    this.isWaterMark = isWaterMark;
  }

  public boolean isNullable() {
    return this.isNullable;
  }

  public void setNullable(boolean isNullable) {
    this.isNullable = isNullable;
  }

  public boolean isUnique() {
    return this.isUnique;
  }

  public void setUnique(boolean isUnique) {
    this.isUnique = isUnique;
  }
}

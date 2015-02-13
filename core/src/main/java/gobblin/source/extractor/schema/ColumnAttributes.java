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

package gobblin.source.extractor.schema;

/**
 * Attributes of column in projection list
 *
 * @author nveeramr
 */
public class ColumnAttributes {
  String columnName;
  String AliasName;
  String sourceTableName;
  String sourceColumnName;

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public String getAliasName() {
    return AliasName;
  }

  public void setAliasName(String aliasName) {
    AliasName = aliasName;
  }

  public String getSourceTableName() {
    return sourceTableName;
  }

  public void setSourceTableName(String sourceTableName) {
    this.sourceTableName = sourceTableName;
  }

  public String getSourceColumnName() {
    return sourceColumnName;
  }

  public void setSourceColumnName(String sourceColumnName) {
    this.sourceColumnName = sourceColumnName;
  }
}

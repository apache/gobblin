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

package org.apache.gobblin.compaction.hive;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Splitter;

import org.apache.gobblin.util.HiveJdbcConnector;


/**
 * A class for managing general Hive tables.
 */
public abstract class HiveTable {

  protected static final String DROP_TABLE_STMT = "DROP TABLE IF EXISTS %1$s";

  protected final String name;
  protected List<String> primaryKeys;
  protected List<HiveAttribute> attributes;

  public static class Builder<T extends Builder<?>> {
    protected String name = UUID.randomUUID().toString().replaceAll("-", "_");
    protected List<String> primaryKeys = new ArrayList<>();
    protected List<HiveAttribute> attributes = new ArrayList<>();

    @SuppressWarnings("unchecked")
    public T withName(String name) {
      if (StringUtils.isNotBlank(name)) {
        this.name = name;
      }
      return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withPrimaryKeys(List<String> primaryKeys) {
      this.primaryKeys = primaryKeys;
      return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withPrimaryKeys(String keyAttrs) {
      List<String> keyAttrsList = Splitter.on(",").trimResults().omitEmptyStrings().splitToList(keyAttrs);
      this.primaryKeys.addAll(keyAttrsList);
      return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withAttributes(List<HiveAttribute> attributes) {
      this.attributes = attributes;
      return (T) this;
    }
  }

  protected HiveTable(HiveTable.Builder<?> builder) {
    this.name = builder.name;
    this.primaryKeys = Collections.unmodifiableList(builder.primaryKeys);
    this.attributes = Collections.unmodifiableList(builder.attributes);
  }

  public String getName() {
    return this.name;
  }

  public List<String> getPrimaryKeys() {
    return this.primaryKeys;
  }

  public List<HiveAttribute> getAttributes() {
    return this.attributes;
  }

  public void dropTable(HiveJdbcConnector conn, String jobId) throws SQLException {
    String dropTableStmt = String.format(DROP_TABLE_STMT, getNameWithJobId(jobId));
    conn.executeStatements(dropTableStmt);
  }

  protected String getNameWithJobId(String randomSuffix) {
    return this.name + "_" + randomSuffix;
  }

  protected boolean hasNoNewColumn(HiveTable table) {
    for (HiveAttribute attribute : table.attributes) {
      if (!this.attributes.contains(attribute)) {
        return false;
      }
    }
    return true;
  }

  public abstract void createTable(HiveJdbcConnector conn, String randomTableSuffix) throws SQLException;

  public abstract HiveTable addNewColumnsInSchema(HiveJdbcConnector conn, HiveTable table, String randomSuffix)
      throws SQLException;
}

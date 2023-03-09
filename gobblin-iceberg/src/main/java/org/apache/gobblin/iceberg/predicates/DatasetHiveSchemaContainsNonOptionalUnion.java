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

package org.apache.gobblin.iceberg.predicates;

import java.io.IOException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Optional;

import gobblin.configuration.State;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.dataset.Dataset;
import org.apache.gobblin.hive.HiveRegister;
import org.apache.gobblin.hive.HiveTable;
import org.apache.gobblin.hive.metastore.HiveMetaStoreUtils;
import org.apache.gobblin.util.function.CheckedExceptionPredicate;


/**
 * Determines if a dataset's hive schema contains a non optional union
 */
@Slf4j
public class DatasetHiveSchemaContainsNonOptionalUnion<T extends Dataset> implements CheckedExceptionPredicate<T, IOException> {
  private final HiveRegister hiveRegister;
  private final Pattern pattern;

  public static final String PREFIX = DatasetHiveSchemaContainsNonOptionalUnion.class.getName();
  /**
   * 1st match group is assumed to be the DB and the 2nd match group the Table for the pattern
   */
  public static final String PATTERN = PREFIX + ".db.table.pattern";

  public DatasetHiveSchemaContainsNonOptionalUnion(Properties properties) {
    this.hiveRegister = getHiveRegister(new State(properties));
    this.pattern = Pattern.compile(properties.getProperty(PATTERN));
  }

  @Override
  public boolean test(T dataset) throws IOException {
    Optional<HiveTable> hiveTable = getTable(dataset);
    if (!hiveTable.isPresent()) {
      log.error("No matching table for dataset={}", dataset);
      return false;
    }

    return containsNonOptionalUnion(hiveTable.get());
  }

  private Optional<HiveTable> getTable(T dataset) throws IOException {
    DbAndTable dbAndTable = getDbAndTable(dataset);
    return this.hiveRegister.getTable(dbAndTable.getDb(), dbAndTable.getTable());
  }

  private DbAndTable getDbAndTable(T dataset) {
    Matcher m = pattern.matcher(dataset.getUrn());
    if (!m.matches() || m.groupCount() != 2) {
      throw new IllegalStateException(String.format("Dataset urn [%s] doesn't follow expected pattern. " +
      "Expected pattern = %s", dataset.getUrn(), pattern.pattern()));
    }
    return new DbAndTable(m.group(1), m.group(2));
  }

  boolean containsNonOptionalUnion(HiveTable table) {
    return HiveMetaStoreUtils.containsNonOptionalUnionTypeColumn(table);
  }

  private HiveRegister getHiveRegister(State state){
    return HiveRegister.get(state);
  }

  @Data
  private static class DbAndTable {
    private final String db;
    private final String table;
  }
}

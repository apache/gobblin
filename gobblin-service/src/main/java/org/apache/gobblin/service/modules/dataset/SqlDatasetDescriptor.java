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

package org.apache.gobblin.service.modules.dataset;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import com.google.common.base.Enums;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorConfigKeys;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.PathUtils;


@Slf4j
@ToString (exclude = {"rawConfig"})
@EqualsAndHashCode (exclude = {"rawConfig"}, callSuper = true)
public class SqlDatasetDescriptor extends BaseDatasetDescriptor implements DatasetDescriptor {
  protected static final String SEPARATION_CHAR = ";";

  protected final String databaseName;
  protected final String tableName;

  @Getter
  private final String path;
  @Getter
  @Setter
  private Config rawConfig;

  public enum Platform {
    SQLSERVER("sqlserver"),
    MYSQL("mysql"),
    ORACLE("oracle"),
    POSTGRES("postgres"),
    TERADARA("teradata");

    private final String platform;

    Platform(final String platform) {
      this.platform = platform;
    }

    @Override
    public String toString() {
      return this.platform;
    }
  }

  public SqlDatasetDescriptor(Config config) throws IOException {
    super(config);
    if (!isPlatformValid()) {
      throw new IOException("Invalid platform specified for SqlDatasetDescriptor: " + getPlatform());
    }
    this.databaseName = ConfigUtils.getString(config, DatasetDescriptorConfigKeys.DATABASE_KEY, ".*");
    this.tableName = ConfigUtils.getString(config, DatasetDescriptorConfigKeys.TABLE_KEY, ".*");
    this.path = fullyQualifiedTableName(this.databaseName, this.tableName);
    this.rawConfig = config.withValue(DatasetDescriptorConfigKeys.PATH_KEY, ConfigValueFactory.fromAnyRef(this.path)).withFallback(super.getRawConfig());
  }

  private String fullyQualifiedTableName(String databaseName, String tableName) {
    return Joiner.on(SEPARATION_CHAR).join(databaseName, tableName);
  }

  protected boolean isPlatformValid() {
    return Enums.getIfPresent(Platform.class, getPlatform().toUpperCase()).isPresent();
  }

  /**
   * Check if the dbName and tableName specified in {@param other}'s path are accepted by the set of dbName.tableName
   * combinations defined by the current {@link SqlDatasetDescriptor}. For example, let:
   * this.path = "test_.*;test_table_.*". Then:
   * isPathContaining("test_db1;test_table_1") = true
   * isPathContaining("testdb1;test_table_2") = false
   *
   * NOTE: otherPath cannot be a globPattern. So:
   * isPathContaining("test_db.*;test_table_*") = false
   *
   * @param other whose path should be in the format of dbName.tableName
   */
  @Override
  protected boolean isPathContaining(DatasetDescriptor other) {
    String otherPath = other.getPath();
    if (otherPath == null) {
      return false;
    }

    if (PathUtils.GLOB_TOKENS.matcher(otherPath).find()) {
      return false;
    }

    //Extract the dbName and tableName from otherPath
    List<String> parts = Splitter.on(SEPARATION_CHAR).splitToList(otherPath);
    if (parts.size() != 2) {
      return false;
    }

    String otherDbName = parts.get(0);
    String otherTableName = parts.get(1);

    return Pattern.compile(this.databaseName).matcher(otherDbName).matches() && Pattern.compile(this.tableName).matcher(otherTableName).matches();
  }
}

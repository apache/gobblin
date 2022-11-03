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

import com.google.common.base.Splitter;
import com.typesafe.config.Config;

import lombok.EqualsAndHashCode;


@EqualsAndHashCode(callSuper = true)
public class IcebergDatasetDescriptor extends SqlDatasetDescriptor {
  public IcebergDatasetDescriptor(Config config)
      throws IOException {
    super(config);
  }

  @Override
  protected boolean isPlatformValid() {
    return "iceberg".equalsIgnoreCase(getPlatform());
  }

  @Override
  protected boolean isPathContaining(DatasetDescriptor other) {
    String otherPath = other.getPath();
    if (otherPath == null) {
      return false;
    }

    //Extract the dbName and tableName from otherPath
    List<String> parts = Splitter.on(SEPARATION_CHAR).splitToList(otherPath);
    if (parts.size() != 2) {
      return false;
    }

    String otherDbName = parts.get(0);
    String otherTableName = parts.get(1);

    return this.databaseName.equals(otherDbName) && this.tableName.equals(otherTableName);
  }
}

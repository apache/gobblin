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

package gobblin.metastore.util;

import org.apache.commons.io.FilenameUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import lombok.AllArgsConstructor;
import lombok.Getter;


@AllArgsConstructor
public class StateStoreTableInfo {
  public static final String CURRENT_NAME = "current";
  public static final String TABLE_PREFIX_SEPARATOR = "-";

  @Getter
  private String prefix;

  @Getter
  private boolean isCurrent;

  public static StateStoreTableInfo get(String tableName) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "Table name is null or empty.");
    String name = FilenameUtils.getBaseName(tableName);
    if (CURRENT_NAME.equalsIgnoreCase(name)) {
      return new StateStoreTableInfo("", true);
    }
    int suffixIndex = name.lastIndexOf(TABLE_PREFIX_SEPARATOR);
    String prefix = "";
    String suffix = "";
    if (suffixIndex >= 0) {
      prefix = name.substring(0, suffixIndex);
      suffix = suffixIndex < name.length() - 1 ? name.substring(suffixIndex + 1) : "";
    }
    if (CURRENT_NAME.equalsIgnoreCase(suffix)) {
      return new StateStoreTableInfo(prefix, true);
    }
    return new StateStoreTableInfo(prefix, false);
  }
}

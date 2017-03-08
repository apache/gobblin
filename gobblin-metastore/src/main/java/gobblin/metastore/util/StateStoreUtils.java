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

import com.google.common.base.Strings;

import gobblin.util.Id;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class StateStoreUtils {
  private StateStoreUtils() {
  }

  public static String getLatestStateKey(String storeName, String urn, String tableName, String latestTableName) {
    if (latestTableName == null) {
      log.debug("Latest table for {}/{}* set to {}", storeName, urn, tableName);
        latestTableName = tableName;
    } else {
      Id latestId = getId(urn, latestTableName);
      Id newId = getId(urn, tableName);
      if (newId.getSequence().compareTo(latestId.getSequence()) > 0) {
        log.debug("Latest table for {}/{}* set to {} instead of {}", storeName, urn, tableName, latestTableName);
          latestTableName = tableName;
      } else {
        log.debug("Latest table for {}/{}* left as {}. Previous table {} is being ignored", storeName, urn, latestTableName, tableName);
      }
    }
    return latestTableName;
  }

  public static Id getId(String urn, String tableName) {
    String id = FilenameUtils.removeExtension(tableName);
    if (!Strings.isNullOrEmpty(urn)) {
      id = id.substring(urn.length() + 1);
    }
    return Id.parse(id);
  }
}

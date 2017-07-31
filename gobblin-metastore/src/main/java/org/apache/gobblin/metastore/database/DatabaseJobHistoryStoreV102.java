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

package org.apache.gobblin.metastore.database;

import org.apache.gobblin.metastore.JobHistoryStore;

import java.io.IOException;

import org.apache.gobblin.rest.JobExecutionInfo;


/**
 * An implementation of {@link JobHistoryStore} backed by MySQL.
 *
 * <p>
 *     The DDLs for the MySQL job history store can be found under metastore/src/main/resources.
 * </p>
 *
 * @author Joel Baranick
 */
@SupportedDatabaseVersion(isDefault = false, version = "1.0.2")
public class DatabaseJobHistoryStoreV102 extends DatabaseJobHistoryStoreV101 implements VersionedDatabaseJobHistoryStore {

  @Override
  protected String getLauncherType(JobExecutionInfo info) {
    if (info.hasLauncherType()) {
      return info.getLauncherType().name();
    }
    return null;
  }
}

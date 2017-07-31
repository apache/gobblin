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

package org.apache.gobblin.data.management.trash;

import java.util.Properties;

import org.apache.hadoop.fs.FileStatus;


/**
 * Policy that will delete the first snapshot, and preserve all other snapshots.
 */
public class TestCleanupPolicy implements SnapshotCleanupPolicy {

  boolean deletedOne = false;

  public TestCleanupPolicy(Properties properties) {
  }

  @Override
  public boolean shouldDeleteSnapshot(FileStatus snapshot, Trash trash) {
    if(this.deletedOne) {
      return false;
    }
    this.deletedOne = true;
    return true;
  }
}

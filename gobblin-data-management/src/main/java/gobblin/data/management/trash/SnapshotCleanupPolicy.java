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

package gobblin.data.management.trash;

import org.apache.hadoop.fs.FileStatus;

/**
 * Policy for determining whether a {@link gobblin.data.management.trash.Trash} snapshot should be deleted.
 */
public interface SnapshotCleanupPolicy {
  /**
   * Decide whether a trash snapshot should be permanently deleted from the file system.
   *
   * <p>
   *   This method will be called for all snapshots in the trash directory, in order from oldest to newest.
   * </p>
   *
   * @param snapshot {@link org.apache.hadoop.fs.FileStatus} of candidate snapshot for deletion.
   * @param trash {@link gobblin.data.management.trash.Trash} object that called this method.
   * @return true if the snapshot should be deleted permanently.
   */
  boolean shouldDeleteSnapshot(FileStatus snapshot, Trash trash);
}

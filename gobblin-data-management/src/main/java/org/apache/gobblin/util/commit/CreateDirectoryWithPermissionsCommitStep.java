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

package org.apache.gobblin.util.commit;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.commit.CommitStep;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.filesystem.OwnerAndPermission;


/**
 * An implementation of {@link CommitStep} for creating directories and their associated permissions before commit
 * Necessary when creating large file paths e.g. Manifest distcp where multiple threads are creating directories at the same time,
 * which can lead to some race conditions described in {@link org.apache.gobblin.util.HadoopUtils#unsafeRenameIfNotExists(FileSystem, Path, Path)}
 * Current implementation only sets permissions, but it is capable of setting owner and group as well.
 */
@Slf4j
public class CreateDirectoryWithPermissionsCommitStep implements CommitStep {
  @Getter
  Map<String, List<OwnerAndPermission>> pathAndPermissions;
  private final URI fsUri;
  private boolean isCompleted = false;

  public CreateDirectoryWithPermissionsCommitStep(FileSystem targetFs, Map<String, List<OwnerAndPermission>> pathAndPermissions) {
    this.pathAndPermissions = pathAndPermissions;
    this.fsUri = targetFs.getUri();
  }

  @Override
  public boolean isCompleted() throws IOException {
    return isCompleted;
  }

  @Override
  public void execute() throws IOException {
    FileSystem fs = FileSystem.get(this.fsUri, new Configuration());

    for (Map.Entry<String, List<OwnerAndPermission>> entry : pathAndPermissions.entrySet()) {
      Path path = new Path(entry.getKey());
      try {
        // Is a no-op if directory already exists, stops when it hits first parent
        // Sets the execute bit for USER in order to rename files to the folder, so it should be reset after this step is completed
        HadoopUtils.ensureDirectoryExists(fs, path, entry.getValue().iterator());
      } catch (AccessControlException e) {
        log.warn("Error while setting permission on " + path, e);
        break;
      }
    }

    isCompleted = true;
  }
}
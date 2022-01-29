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
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.commit.CommitStep;
import org.apache.gobblin.data.management.copy.OwnerAndPermission;

/**
 * An implementation of {@link CommitStep} for setting any file permissions.
 * Current implementation only sets permissions, but it is capable of setting owner and group as well.
 */
@Slf4j
public class SetPermissionCommitStep implements CommitStep {
  Map<String, OwnerAndPermission> pathAndPermissions;
  private final URI fsUri;
  public final boolean stopOnError;
  public static final String STOP_ON_ERROR_KEY = "stop.on.error";
  public static final String DEFAULT_STOP_ON_ERROR = "false";
  private boolean isCompleted = false;

  public SetPermissionCommitStep(FileSystem targetFs, Map<String, OwnerAndPermission> pathAndPermissions,
      Properties props) {
    this.pathAndPermissions = pathAndPermissions;
    this.fsUri = targetFs.getUri();
    this.stopOnError = Boolean.parseBoolean(props.getProperty(STOP_ON_ERROR_KEY, DEFAULT_STOP_ON_ERROR));
  }

  @Override
  public boolean isCompleted() throws IOException {
    return isCompleted;
  }

  @Override
  public void execute() throws IOException {
    FileSystem fs = FileSystem.get(this.fsUri, new Configuration());

    for (Map.Entry<String, OwnerAndPermission> entry : pathAndPermissions.entrySet()) {
      Path path = new Path(entry.getKey());
      try {
        log.info("Setting permission {} on path {}", entry.getValue().getFsPermission(), path);
        fs.setPermission(path, entry.getValue().getFsPermission());
        // TODO : we can also set owner and group here.
      } catch (AccessControlException e) {
        log.warn("Error while setting permission on " + path, e);
        if (this.stopOnError) {
          log.info("Skip setting rest of the permissions because stopOnError is true.");
          break;
        }
      }
    }

    isCompleted = true;
  }
}
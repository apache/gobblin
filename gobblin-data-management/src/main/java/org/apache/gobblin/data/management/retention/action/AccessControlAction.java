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
package org.apache.gobblin.data.management.retention.action;

import java.io.IOException;
import java.util.List;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.typesafe.config.Config;

import org.apache.gobblin.data.management.policy.VersionSelectionPolicy;
import org.apache.gobblin.data.management.version.DatasetVersion;
import org.apache.gobblin.data.management.version.FileStatusAware;
import org.apache.gobblin.data.management.version.FileSystemDatasetVersion;
import org.apache.gobblin.util.ConfigUtils;


/**
 * A {@link RetentionAction} that is used to change the permissions/owner/group of a {@link FileSystemDatasetVersion}
 */
@Slf4j
public class AccessControlAction extends RetentionAction {

  /**
   * Optional - The permission mode to set on selected versions either in octal or symbolic format. E.g 750
   */
  private static final String MODE_KEY = "mode";
  /**
   * Optional - The owner to set on selected versions
   */
  private static final String OWNER_KEY = "owner";
  /**
   * Optional - The group to set on selected versions
   */
  private static final String GROUP_KEY = "group";

  private final Optional<FsPermission> permission;
  private final Optional<String> owner;
  private final Optional<String> group;

  @VisibleForTesting
  @Getter
  private final VersionSelectionPolicy<DatasetVersion> selectionPolicy;

  @VisibleForTesting
  AccessControlAction(Config actionConfig, FileSystem fs, Config jobConfig) {
    super(actionConfig, fs, jobConfig);
    this.permission = actionConfig.hasPath(MODE_KEY) ? Optional.of(new FsPermission(actionConfig.getString(MODE_KEY))) : Optional
            .<FsPermission> absent();
    this.owner = Optional.fromNullable(ConfigUtils.getString(actionConfig, OWNER_KEY, null));
    this.group = Optional.fromNullable(ConfigUtils.getString(actionConfig, GROUP_KEY, null));
    this.selectionPolicy = createSelectionPolicy(actionConfig, jobConfig);

  }

  /**
   * Applies {@link #selectionPolicy} on <code>allVersions</code> and modifies permission/owner to the selected {@link DatasetVersion}s
   * where necessary.
   * <p>
   * This action only available for {@link FileSystemDatasetVersion}. It simply skips the operation if a different type
   * of {@link DatasetVersion} is passed.
   * </p>
   * {@inheritDoc}
   * @see org.apache.gobblin.data.management.retention.action.RetentionAction#execute(java.util.List)
   */
  @Override
  public void execute(List<DatasetVersion> allVersions) throws IOException {
    // Select version on which access control actions need to performed
    for (DatasetVersion datasetVersion : this.selectionPolicy.listSelectedVersions(allVersions)) {
      executeOnVersion(datasetVersion);
    }
  }

  private void executeOnVersion(DatasetVersion datasetVersion) throws IOException {
    // Perform action if it is a FileSystemDatasetVersion
    if (datasetVersion instanceof FileSystemDatasetVersion) {
      FileSystemDatasetVersion fsDatasetVersion = (FileSystemDatasetVersion) datasetVersion;

      // If the version is filestatus aware, use the filestatus to ignore permissions update when the path already has
      // the desired permissions
      if (datasetVersion instanceof FileStatusAware) {
        for (FileStatus fileStatus : ((FileStatusAware)datasetVersion).getFileStatuses()) {
          if (needsPermissionsUpdate(fileStatus) || needsOwnerUpdate(fileStatus) || needsGroupUpdate(fileStatus)) {
            updatePermissionsAndOwner(fileStatus.getPath());
          }
        }
      } else {
        for (Path path : fsDatasetVersion.getPaths()) {
          updatePermissionsAndOwner(path);
        }
      }
    }
  }

  private boolean needsPermissionsUpdate(FileStatus fileStatus) {
    return this.permission.isPresent() && !this.permission.get().equals(fileStatus.getPermission());
  }

  private boolean needsOwnerUpdate(FileStatus fileStatus) {
    return this.owner.isPresent() && !StringUtils.equals(owner.get(), fileStatus.getOwner());
  }

  private boolean needsGroupUpdate(FileStatus fileStatus) {
    return this.group.isPresent() && !StringUtils.equals(group.get(), fileStatus.getGroup());
  }

  private void updatePermissionsAndOwner(Path path) throws IOException {
    boolean atLeastOneOperationFailed = false;
    if (this.fs.exists(path)) {

      try {
        // Update permissions if set in config
        if (this.permission.isPresent()) {
          if (!this.isSimulateMode) {
            this.fs.setPermission(path, this.permission.get());
            log.debug("Set permissions for {} to {}", path, this.permission.get());
          } else {
            log.info("Simulating set permissions for {} to {}", path, this.permission.get());
          }
        }
      } catch (IOException e) {
        log.error(String.format("Setting permissions failed on %s", path), e);
        atLeastOneOperationFailed = true;
      }

      // Update owner and group if set in config
      if (this.owner.isPresent() || this.group.isPresent()) {
        if (!this.isSimulateMode) {
          this.fs.setOwner(path, this.owner.orNull(), this.group.orNull());
          log.debug("Set owner and group for {} to {}:{}", path, this.owner.orNull(),
              this.group.orNull());
        } else {
          log.info("Simulating set owner and group for {} to {}:{}", path, this.owner.orNull(),
              this.group.orNull());
        }
      }

      if (atLeastOneOperationFailed) {
        throw new RuntimeException(String.format(
            "At least one failure happened while processing %s. Look for previous logs for failures", path));
      }
    }
  }
}

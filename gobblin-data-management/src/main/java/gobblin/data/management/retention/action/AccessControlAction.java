/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.data.management.retention.action;

import java.io.IOException;
import java.util.List;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.typesafe.config.Config;

import gobblin.data.management.policy.VersionSelectionPolicy;
import gobblin.data.management.version.DatasetVersion;
import gobblin.data.management.version.FileSystemDatasetVersion;
import gobblin.util.ConfigUtils;


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

  private final Optional<String> permission;
  private final Optional<String> owner;
  private final Optional<String> group;

  @VisibleForTesting
  @Getter
  private final VersionSelectionPolicy<DatasetVersion> selectionPolicy;


  @VisibleForTesting
  AccessControlAction(Config actionConfig, FileSystem fs) {
    super(actionConfig, fs);
    this.permission = Optional.fromNullable(ConfigUtils.getString(actionConfig, MODE_KEY, null));
    this.owner = Optional.fromNullable(ConfigUtils.getString(actionConfig, OWNER_KEY, null));
    this.group = Optional.fromNullable(ConfigUtils.getString(actionConfig, GROUP_KEY, null));
    this.selectionPolicy = createSelectionPolicy(actionConfig);

  }

  /**
   * Applies {@link #selectionPolicy} on <code>allVersions</code> and modifies permission/owner to the selected {@link DatasetVersion}s
   * where necessary.
   * <p>
   * This action only available for {@link FileSystemDatasetVersion}. It simply skips the operation if a different type
   * of {@link DatasetVersion} is passed.
   * </p>
   * {@inheritDoc}
   * @see gobblin.data.management.retention.action.RetentionAction#execute(java.util.List)
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
      for (Path path : fsDatasetVersion.getPaths()) {

        if (this.fs.exists(path)) {

          // Update permissions if set in config
          if (this.permission.isPresent()) {
            FsPermission fsPermission = new FsPermission(this.permission.get());
            this.fs.setPermission(path, fsPermission);
            log.debug("Set permissions for {} to {}", path.toString(), fsPermission);
          }

          // Update owner and group if set in config
          if (this.owner.isPresent() || this.group.isPresent()) {
            this.fs.setOwner(path, this.owner.orNull(), this.group.orNull());
            log.debug("Set owner and group for {} to {}:{}", path.toString(), this.owner.orNull(),
                this.group.orNull());
          }
        }
      }
    }
  }
}

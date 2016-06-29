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
package gobblin.data.management.policy;

import java.util.Collection;
import java.util.List;

import lombok.AllArgsConstructor;

import gobblin.data.management.retention.policy.RetentionPolicy;
import gobblin.data.management.version.FileSystemDatasetVersion;

/**
 * A wrapper {@link VersionSelectionPolicy} that delegates calls to deprecated {@link RetentionPolicy}
 */
@AllArgsConstructor
public class EmbeddedRetentionSelectionPolicy<T extends FileSystemDatasetVersion> implements VersionSelectionPolicy<T> {

  private final RetentionPolicy<T> embeddedRetentionPolicy;

  @SuppressWarnings("unchecked")
  @Override
  public Class<? extends FileSystemDatasetVersion> versionClass() {
    return (Class<? extends FileSystemDatasetVersion>) this.embeddedRetentionPolicy.versionClass();
  }

  @SuppressWarnings("deprecation")
  @Override
  public Collection<T> listSelectedVersions(List<T> allVersions) {
    return this.embeddedRetentionPolicy.listDeletableVersions(allVersions);
  }
}

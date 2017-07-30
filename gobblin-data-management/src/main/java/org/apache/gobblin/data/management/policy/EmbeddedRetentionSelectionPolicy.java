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
package gobblin.data.management.policy;

import java.util.Collection;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.ToString;

import gobblin.data.management.retention.policy.RetentionPolicy;
import gobblin.data.management.version.FileSystemDatasetVersion;

/**
 * A wrapper {@link VersionSelectionPolicy} that delegates calls to deprecated {@link RetentionPolicy}
 */
@AllArgsConstructor
@ToString
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

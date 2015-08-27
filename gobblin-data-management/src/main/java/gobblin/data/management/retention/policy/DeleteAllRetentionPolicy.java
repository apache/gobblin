/*
* Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License"); you may not use
* this file except in compliance with the License. You may obtain a copy of the
* License at  http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software distributed
* under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
* CONDITIONS OF ANY KIND, either express or implied.
*/

package gobblin.data.management.retention.policy;

import java.util.Collection;
import java.util.List;
import java.util.Properties;

import gobblin.data.management.retention.version.DatasetVersion;

/**
 * Implementation of {@link RetentionPolicy} that marks all {@link DatasetVersion}s as deletable.
 */
public class DeleteAllRetentionPolicy implements RetentionPolicy<DatasetVersion> {

  public DeleteAllRetentionPolicy(Properties properties) {
  }

  @Override
  public Class<? extends DatasetVersion> versionClass() {
    return DatasetVersion.class;
  }

  @Override
  public Collection<DatasetVersion> listDeletableVersions(List<DatasetVersion> allVersions) {
    return allVersions;
  }
}

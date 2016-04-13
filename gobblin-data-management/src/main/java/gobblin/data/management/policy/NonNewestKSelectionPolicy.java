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
import java.util.Properties;

import org.apache.commons.collections.CollectionUtils;

import com.typesafe.config.Config;

import gobblin.data.management.version.FileSystemDatasetVersion;


/**
 * Select the all the versions except the newest k versions of the dataset. And inverse of {@link NewestKSelectionPolicy}
 */
public class NonNewestKSelectionPolicy extends NewestKSelectionPolicy {

  public NonNewestKSelectionPolicy(int versionsRetained) {
    super(versionsRetained);
  }

  public NonNewestKSelectionPolicy(Properties props) {
    super(props);
  }

  public NonNewestKSelectionPolicy(Config config) {
    super(config);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Collection<FileSystemDatasetVersion> listSelectedVersions(List<FileSystemDatasetVersion> allVersions) {
    return CollectionUtils.subtract(allVersions, super.listSelectedVersions(allVersions));
  }
}

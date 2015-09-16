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

package gobblin.data.management.retention.test;

import gobblin.data.management.retention.policy.RetentionPolicy;
import gobblin.data.management.retention.version.DatasetVersion;
import gobblin.data.management.retention.version.StringDatasetVersion;

import java.util.Collection;
import java.util.List;
import java.util.Properties;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;


/**
* RetentionPolivy that deletes versions containing a particular string token.
*/
public abstract class ContainsStringRetentionPolicy implements RetentionPolicy<StringDatasetVersion> {
  public ContainsStringRetentionPolicy(Properties props) {
  }

  @Override public Class<? extends DatasetVersion> versionClass() {
    return StringDatasetVersion.class;
  }

  @Override public Collection<StringDatasetVersion> listDeletableVersions(List<StringDatasetVersion> allVersions) {
    return Lists.newArrayList(Iterables.filter(allVersions, new Predicate<StringDatasetVersion>() {
      @Override public boolean apply(StringDatasetVersion input) {
        return input.getVersion().contains(getSearchToken());
      }
    }));
  }

  protected abstract String getSearchToken();
}

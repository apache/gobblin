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

package gobblin.data.management.retention;

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

import gobblin.data.management.retention.policy.NewestKRetentionPolicy;
import gobblin.data.management.retention.version.DatasetVersion;
import gobblin.data.management.retention.version.StringDatasetVersion;


public class NewestKRetentionPolicyTest {

  @Test
  public void test() {

    Properties props = new Properties();

    NewestKRetentionPolicy policy = new NewestKRetentionPolicy(props);

    StringDatasetVersion datasetVersion1 = new StringDatasetVersion("000_newest", new Path("test"));
    StringDatasetVersion datasetVersion2 = new StringDatasetVersion("001_mid", new Path("test"));
    StringDatasetVersion datasetVersion3 = new StringDatasetVersion("002_oldest", new Path("test"));


    Assert.assertEquals(policy.versionClass(), DatasetVersion.class);

    List<DatasetVersion> versions = Lists.newArrayList();
    versions.add(datasetVersion1);
    versions.add(datasetVersion2);
    versions.add(datasetVersion3);
    List<DatasetVersion> deletableVersions = Lists.newArrayList(policy.listDeletableVersions(versions));
    Assert.assertEquals(deletableVersions.size(),1);
    Assert.assertEquals(((StringDatasetVersion) deletableVersions.get(0)).getVersion(), "002_oldest");

  }

}

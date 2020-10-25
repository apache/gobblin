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

package org.apache.gobblin.data.management.retention;

import org.apache.gobblin.data.management.retention.policy.CombineRetentionPolicy;
import org.apache.gobblin.data.management.retention.test.ContainsARetentionPolicy;
import org.apache.gobblin.data.management.retention.test.ContainsBRetentionPolicy;
import org.apache.gobblin.data.management.retention.test.ContainsCRetentionPolicy;
import org.apache.gobblin.data.management.version.DatasetVersion;
import org.apache.gobblin.data.management.version.FileStatusDatasetVersion;
import org.apache.gobblin.data.management.version.StringDatasetVersion;
import org.apache.gobblin.data.management.version.TimestampedDatasetVersion;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;


public class CombineRetentionPolicyTest {

  @Test
  public void testConfig() throws IOException {
    Properties props = new Properties();

    props.setProperty(CombineRetentionPolicy.COMBINE_RETENTION_POLICIES, "ContainsA,"
        + ContainsBRetentionPolicy.class.getCanonicalName() + "," + ContainsCRetentionPolicy.class.getCanonicalName());
    props.setProperty(CombineRetentionPolicy.DELETE_SETS_COMBINE_OPERATION,
        CombineRetentionPolicy.DeletableCombineOperation.UNION.name());

    CombineRetentionPolicy<DatasetVersion> policy = new CombineRetentionPolicy<>(props);

    Collection<DatasetVersion> deletableVersions = policy.listDeletableVersions(Lists
        .newArrayList(new StringDatasetVersion("a", new Path("/")),
            new StringDatasetVersion("abc", new Path("/")), new StringDatasetVersion("abcd", new Path("/")),
            new StringDatasetVersion("bc", new Path("/")), new StringDatasetVersion("d", new Path("/"))));

    Set<String> actualDeletableVersions =
        deletableVersions.stream().map(input -> ((StringDatasetVersion) input).getVersion()).collect(Collectors.toSet());

    Assert.assertEquals(policy.versionClass(), StringDatasetVersion.class);
    Assert.assertEquals(deletableVersions.size(), 4);
    Assert.assertEquals(actualDeletableVersions, Sets.newHashSet("abcd", "abc", "a", "bc"));
  }

  @Test
  public void testIntersect() throws IOException {
    Properties props = new Properties();

    props.setProperty(CombineRetentionPolicy.RETENTION_POLICIES_PREFIX + "1",
        ContainsARetentionPolicy.class.getCanonicalName());
    props.setProperty(CombineRetentionPolicy.RETENTION_POLICIES_PREFIX + "2",
        ContainsBRetentionPolicy.class.getCanonicalName());
    props.setProperty(CombineRetentionPolicy.RETENTION_POLICIES_PREFIX + "3",
        ContainsCRetentionPolicy.class.getCanonicalName());
    props.setProperty(CombineRetentionPolicy.DELETE_SETS_COMBINE_OPERATION,
        CombineRetentionPolicy.DeletableCombineOperation.INTERSECT.name());

    CombineRetentionPolicy<DatasetVersion> policy = new CombineRetentionPolicy<>(props);

    Collection<DatasetVersion> deletableVersions = policy.listDeletableVersions(Lists
            .newArrayList(new StringDatasetVersion("a", new Path("/")),
                new StringDatasetVersion("abc", new Path("/")), new StringDatasetVersion("abcd", new Path("/")),
                new StringDatasetVersion("bc", new Path("/")), new StringDatasetVersion("d", new Path("/"))));

    Set<String> actualDeletableVersions =
        deletableVersions.stream().map(input -> ((StringDatasetVersion) input).getVersion()).collect(Collectors.toSet());

    Assert.assertEquals(policy.versionClass(), StringDatasetVersion.class);
    Assert.assertEquals(deletableVersions.size(), 2);
    Assert.assertEquals(actualDeletableVersions, Sets.newHashSet("abcd", "abc"));

  }

  @Test
  public void testUnion() throws IOException {
    Properties props = new Properties();

    props.setProperty(CombineRetentionPolicy.RETENTION_POLICIES_PREFIX + "1",
        ContainsARetentionPolicy.class.getCanonicalName());
    props.setProperty(CombineRetentionPolicy.RETENTION_POLICIES_PREFIX + "2",
        ContainsBRetentionPolicy.class.getCanonicalName());
    props.setProperty(CombineRetentionPolicy.RETENTION_POLICIES_PREFIX + "3",
        ContainsCRetentionPolicy.class.getCanonicalName());
    props.setProperty(CombineRetentionPolicy.DELETE_SETS_COMBINE_OPERATION,
        CombineRetentionPolicy.DeletableCombineOperation.UNION.name());

    CombineRetentionPolicy<DatasetVersion> policy = new CombineRetentionPolicy<>(props);

    Collection<DatasetVersion> deletableVersions = policy.listDeletableVersions(Lists
        .newArrayList(new StringDatasetVersion("a", new Path("/")),
            new StringDatasetVersion("abc", new Path("/")), new StringDatasetVersion("abcd", new Path("/")),
            new StringDatasetVersion("bc", new Path("/")), new StringDatasetVersion("d", new Path("/"))));

    Set<String> actualDeletableVersions =
        deletableVersions.stream().map(input -> ((StringDatasetVersion) input).getVersion())
            .collect(Collectors.toSet());

    Assert.assertEquals(deletableVersions.size(), 4);
    Assert.assertEquals(actualDeletableVersions, Sets.newHashSet("abcd", "abc", "a", "bc"));

  }

  @Test
  public void testCommonSuperclass() throws IOException {
    Properties props = new Properties();

    props.setProperty(CombineRetentionPolicy.RETENTION_POLICIES_PREFIX + "1",
        ContainsARetentionPolicy.class.getCanonicalName());
    props.setProperty(CombineRetentionPolicy.DELETE_SETS_COMBINE_OPERATION,
        CombineRetentionPolicy.DeletableCombineOperation.INTERSECT.name());

    CombineRetentionPolicy policy = new CombineRetentionPolicy(props);

    Assert.assertEquals(policy.commonSuperclass(StringDatasetVersion.class, StringDatasetVersion.class),
        StringDatasetVersion.class);
    Assert.assertEquals(policy.commonSuperclass(StringDatasetVersion.class, TimestampedDatasetVersion.class),
        DatasetVersion.class);
    Assert.assertEquals(policy.commonSuperclass(StringDatasetVersion.class, FileStatusDatasetVersion.class),
        StringDatasetVersion.class);
    Assert.assertEquals(policy.commonSuperclass(FileStatusDatasetVersion.class, StringDatasetVersion.class),
        StringDatasetVersion.class);
    Assert.assertEquals(policy.commonSuperclass(DatasetVersion.class, StringDatasetVersion.class),
        DatasetVersion.class);
  }

}

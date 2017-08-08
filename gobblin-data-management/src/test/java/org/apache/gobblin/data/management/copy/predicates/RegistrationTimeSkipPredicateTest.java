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

package org.apache.gobblin.data.management.copy.predicates;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;

import org.apache.gobblin.data.management.copy.CopyConfiguration;
import org.apache.gobblin.data.management.copy.CopyContext;
import org.apache.gobblin.data.management.copy.hive.HiveCopyEntityHelper;
import org.apache.gobblin.data.management.copy.hive.HiveDataset;
import org.apache.gobblin.data.management.copy.hive.HivePartitionFileSet;


public class RegistrationTimeSkipPredicateTest {

  @Test
  public void test() throws Exception {

    Path partition1Path = new Path("/path/to/partition1");
    long modTime = 100000;

    CopyContext copyContext = new CopyContext();
    CopyConfiguration copyConfiguration = Mockito.mock(CopyConfiguration.class);
    Mockito.doReturn(copyContext).when(copyConfiguration).getCopyContext();
    HiveDataset dataset = Mockito.mock(HiveDataset.class);
    FileSystem fs = Mockito.spy(FileSystem.getLocal(new Configuration()));
    FileStatus status = new FileStatus(1, false, 1, 1, modTime, partition1Path);
    Path qualifiedPath = fs.makeQualified(partition1Path);
    Mockito.doReturn(status).when(fs).getFileStatus(qualifiedPath);
    Mockito.doReturn(status).when(fs).getFileStatus(partition1Path);
    Mockito.doReturn(fs).when(dataset).getFs();

    HiveCopyEntityHelper helper = Mockito.mock(HiveCopyEntityHelper.class);
    Mockito.doReturn(copyConfiguration).when(helper).getConfiguration();
    Mockito.doReturn(dataset).when(helper).getDataset();

    RegistrationTimeSkipPredicate predicate = new RegistrationTimeSkipPredicate(helper);

    // partition exists, but registration time before modtime => don't skip
    HivePartitionFileSet pc = createPartitionCopy(partition1Path, modTime - 1, true);
    Assert.assertFalse(predicate.apply(pc));

    // partition exists, registration time equal modtime => don't skip
    pc = createPartitionCopy(partition1Path, modTime, true);
    Assert.assertFalse(predicate.apply(pc));

    // partition exists, registration time larger modtime => do skip
    pc = createPartitionCopy(partition1Path, modTime + 1, true);
    Assert.assertTrue(predicate.apply(pc));

    // partition doesn't exist => don't skip
    pc = createPartitionCopy(partition1Path, modTime + 1, false);
    Assert.assertFalse(predicate.apply(pc));

    // partition exists but is not annotated => don't skip
    pc = createPartitionCopy(partition1Path, modTime + 1, true);
    pc.getExistingTargetPartition().get().getParameters().clear();
    Assert.assertFalse(predicate.apply(pc));

  }

  public HivePartitionFileSet createPartitionCopy(Path location, long registrationGenerationTime,
      boolean targetPartitionExists) {
    HivePartitionFileSet partitionCopy = Mockito.mock(HivePartitionFileSet.class);

    Partition partition = Mockito.mock(Partition.class);
    Mockito.doReturn(location).when(partition).getDataLocation();
    Mockito.doReturn(partition).when(partitionCopy).getPartition();

    if (targetPartitionExists) {

      Partition targetPartition = Mockito.mock(Partition.class);

      Map<String, String> parameters = Maps.newHashMap();
      parameters.put(HiveDataset.REGISTRATION_GENERATION_TIME_MILLIS,
          Long.toString(registrationGenerationTime));
      Mockito.doReturn(parameters).when(targetPartition).getParameters();

      Mockito.doReturn(Optional.of(targetPartition)).when(partitionCopy).getExistingTargetPartition();
    } else {
      Mockito.doReturn(Optional.absent()).when(partitionCopy).getExistingTargetPartition();
    }

    return partitionCopy;
  }

}

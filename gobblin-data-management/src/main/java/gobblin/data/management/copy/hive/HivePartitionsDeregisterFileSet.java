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

package gobblin.data.management.copy.hive;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.Partition;

import com.google.common.collect.Lists;

import gobblin.data.management.copy.CopyEntity;

import lombok.extern.slf4j.Slf4j;


/**
 * A {@link HiveFileSet} for deregistering partitions in the target.
 */
@Slf4j
public class HivePartitionsDeregisterFileSet extends HiveFileSet {

  private final Collection<Partition> partitionsToDeregister;
  private final HiveCopyEntityHelper helper;

  public HivePartitionsDeregisterFileSet(String name, HiveDataset dataset, Collection<Partition> partitionsToDeregister,
      HiveCopyEntityHelper helper) {
    super(name, dataset);
    this.partitionsToDeregister = partitionsToDeregister;
    this.helper = helper;
  }

  @Override
  protected Collection<CopyEntity> generateCopyEntities()
      throws IOException {
    List<CopyEntity> deregisterCopyEntities = Lists.newArrayList();
    int priority = 1;
    for (Partition partition : partitionsToDeregister) {
      try {
        priority = this.helper.addPartitionDeregisterSteps(deregisterCopyEntities, getName(), priority,
            this.helper.getTargetTable(), partition);
      } catch (IOException ioe) {
        log.error(
            "Could not create work unit to deregister partition " + partition.getCompleteName());
      }
    }
    return deregisterCopyEntities;
  }
}

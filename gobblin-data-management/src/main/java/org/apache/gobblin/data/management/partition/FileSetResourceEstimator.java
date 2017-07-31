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

package org.apache.gobblin.data.management.partition;

import com.typesafe.config.Config;

import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.copy.CopyResourcePool;
import org.apache.gobblin.util.request_allocation.ResourceEstimator;
import org.apache.gobblin.util.request_allocation.ResourcePool;
import org.apache.gobblin.util.request_allocation.ResourceRequirement;


/**
 * A {@link ResourceEstimator} that uses a {@link CopyResourcePool} and populates a {@link ResourceRequirement} for a
 * distcp {@link FileSet}.
 */
public class FileSetResourceEstimator implements ResourceEstimator<FileSet<CopyEntity>> {

  static class Factory implements ResourceEstimator.Factory<FileSet<CopyEntity>> {
    @Override
    public ResourceEstimator<FileSet<CopyEntity>> create(Config config) {
      return new FileSetResourceEstimator();
    }
  }

  @Override
  public ResourceRequirement estimateRequirement(FileSet<CopyEntity> copyEntityFileSet, ResourcePool pool) {
    if (!(pool instanceof CopyResourcePool)) {
      throw new IllegalArgumentException("Must use a " + CopyResourcePool.class.getSimpleName());
    }
    CopyResourcePool copyResourcePool = (CopyResourcePool) pool;
    return copyResourcePool.getCopyResourceRequirementBuilder().setEntities(copyEntityFileSet.getTotalEntities())
        .setBytes(copyEntityFileSet.getTotalSizeInBytes()).build();
  }

}

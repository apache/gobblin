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
package org.apache.gobblin.compliance.purger;

import com.google.common.base.Preconditions;


/**
 * Default commit policy for purger.
 *
 * @author adsharma
 */
public class HivePurgerCommitPolicy implements CommitPolicy<PurgeableHivePartitionDataset> {
  /**
   * @return true if the last modified time does not change during the execution of the job.
   */
  public boolean shouldCommit(PurgeableHivePartitionDataset dataset) {
    Preconditions
        .checkNotNull(dataset.getStartTime(), "Start time for purger is not set for dataset " + dataset.datasetURN());
    Preconditions
        .checkNotNull(dataset.getEndTime(), "End time for purger is not set for dataset " + dataset.datasetURN());
    return dataset.getStartTime() == dataset.getEndTime();
  }
}

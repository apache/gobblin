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
package org.apache.gobblin.compliance.retention;

import java.util.List;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.data.management.retention.dataset.CleanableDataset;
import org.apache.gobblin.data.management.retention.version.VersionCleaner;
import org.apache.gobblin.data.management.version.DatasetVersion;


/**
 * An abstract class for handling retention of {@link HivePartitionRetentionVersion}
 *
 * @author adsharma
 */
public abstract class HivePartitionVersionRetentionRunner extends VersionCleaner {
  protected List<String> nonDeletableVersionLocations;
  protected State state;

  public HivePartitionVersionRetentionRunner(CleanableDataset dataset, DatasetVersion version,
      List<String> nonDeletableVersionLocations, State state) {
    super(version, dataset);
    this.state = new State(state);
    this.nonDeletableVersionLocations = nonDeletableVersionLocations;
  }
}

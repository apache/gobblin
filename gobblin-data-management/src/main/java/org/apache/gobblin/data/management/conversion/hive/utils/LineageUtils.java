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
package org.apache.gobblin.data.management.conversion.hive.utils;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.conversion.hive.dataset.ConvertibleHiveDataset;
import org.apache.gobblin.data.management.conversion.hive.source.HiveWorkUnit;
import org.apache.gobblin.data.management.conversion.hive.watermarker.PartitionLevelWatermarker;
import org.apache.gobblin.data.management.copy.hive.HiveDataset;
import org.apache.gobblin.source.workunit.WorkUnit;


/**
 * Utility functions for tracking lineage in hive conversion workflows
 */
public class LineageUtils {
  public static boolean shouldSetLineageInfo(WorkUnit workUnit) {
    // Create a HiveWorkUnit from the workunit
    HiveWorkUnit hiveWorkUnit = new HiveWorkUnit(workUnit);
    if (hiveWorkUnit.getPropAsBoolean(PartitionLevelWatermarker.IS_WATERMARK_WORKUNIT_KEY, false)) {
      return false;
    }
    HiveDataset hiveDataset = hiveWorkUnit.getHiveDataset();
    return hiveDataset instanceof ConvertibleHiveDataset;
  }

  public static boolean shouldSetLineageInfo(WorkUnitState workUnitState) {
    return shouldSetLineageInfo(workUnitState.getWorkunit());
  }

  private LineageUtils() {
    // cant instantiate
  }
}

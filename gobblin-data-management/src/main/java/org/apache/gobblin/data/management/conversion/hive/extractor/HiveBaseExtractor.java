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

package org.apache.gobblin.data.management.conversion.hive.extractor;

import java.io.IOException;
import com.google.common.base.Optional;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.conversion.hive.source.HiveWorkUnit;
import org.apache.gobblin.data.management.conversion.hive.watermarker.PartitionLevelWatermarker;
import org.apache.gobblin.data.management.copy.hive.HiveDataset;
import org.apache.gobblin.data.management.copy.hive.HiveDatasetFinder;
import org.apache.gobblin.hive.HiveMetastoreClientPool;
import org.apache.gobblin.source.extractor.Extractor;


/**
 * Base {@link Extractor} for extracting from {@link org.apache.gobblin.data.management.conversion.hive.source.HiveSource}
 */
public abstract class HiveBaseExtractor<S, D> implements Extractor<S, D> {

  protected HiveWorkUnit hiveWorkUnit;
  protected HiveDataset hiveDataset;
  protected String dbName;
  protected String tableName;
  protected HiveMetastoreClientPool pool;

  public HiveBaseExtractor(WorkUnitState state) throws IOException {
    if (Boolean.valueOf(state.getPropAsBoolean(PartitionLevelWatermarker.IS_WATERMARK_WORKUNIT_KEY))) {
      return;
    }
    this.hiveWorkUnit = new HiveWorkUnit(state.getWorkunit());
    this.hiveDataset = hiveWorkUnit.getHiveDataset();
    this.dbName = hiveDataset.getDbAndTable().getDb();
    this.tableName = hiveDataset.getDbAndTable().getTable();
    this.pool = HiveMetastoreClientPool.get(state.getJobState().getProperties(),
        Optional.fromNullable(state.getJobState().getProp(HiveDatasetFinder.HIVE_METASTORE_URI_KEY)));
  }

  @Override
  public long getExpectedRecordCount() {
    return 1;
  }

  /**
   * Watermark is not managed by this extractor.
   */
  @Override
  public long getHighWatermark() {
    return 0;
  }

  @Override
  public void close() throws IOException {}
}

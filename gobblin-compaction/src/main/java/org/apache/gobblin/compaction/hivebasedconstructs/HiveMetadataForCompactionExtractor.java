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

package org.apache.gobblin.compaction.hivebasedconstructs;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.thrift.TException;
import com.google.common.base.Splitter;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.conversion.hive.watermarker.PartitionLevelWatermarker;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.util.AutoReturnableObject;
import org.apache.gobblin.data.management.conversion.hive.extractor.HiveBaseExtractor;
import lombok.extern.slf4j.Slf4j;


/**
 * {@link Extractor} that extracts primary key field name, delta field name, and location from hive metastore and
 * creates an {@link MRCompactionEntity}
 */
@Slf4j
public class HiveMetadataForCompactionExtractor extends HiveBaseExtractor<Void, MRCompactionEntity> {

  public static final String COMPACTION_PRIMARY_KEY = "hive.metastore.primaryKey";
  public static final String COMPACTION_DELTA = "hive.metastore.delta";

  private MRCompactionEntity compactionEntity;
  private boolean extracted = false;

  public HiveMetadataForCompactionExtractor(WorkUnitState state, FileSystem fs) throws IOException, TException, HiveException {
    super(state);

    if (Boolean.valueOf(state.getPropAsBoolean(PartitionLevelWatermarker.IS_WATERMARK_WORKUNIT_KEY))) {
      log.info("Ignoring Watermark workunit for {}", state.getProp(ConfigurationKeys.DATASET_URN_KEY));
      return;
    }

    try (AutoReturnableObject<IMetaStoreClient> client = this.pool.getClient()) {
      Table table = client.get().getTable(this.dbName, this.tableName);

      String primaryKeyString = table.getParameters().get(state.getProp(COMPACTION_PRIMARY_KEY));
      List<String> primaryKeyList = Splitter.on(',').omitEmptyStrings().trimResults().splitToList(primaryKeyString);

      String deltaString = table.getParameters().get(state.getProp(COMPACTION_DELTA));
      List<String> deltaList = Splitter.on(',').omitEmptyStrings().trimResults().splitToList(deltaString);

      Path dataFilesPath = new Path(table.getSd().getLocation());

      compactionEntity = new MRCompactionEntity(primaryKeyList, deltaList, dataFilesPath, state.getProperties());
    }
  }

  @Override
  public MRCompactionEntity readRecord(MRCompactionEntity reuse) {
    if (!extracted) {
      extracted = true;
      return compactionEntity;
    } else {
      return null;
    }
  }

  @Override
  public Void getSchema() throws IOException {
    return null;
  }
}

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

package org.apache.gobblin.data.management.copy;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.data.management.copy.hive.HiveDataset;
import org.apache.gobblin.data.management.copy.hive.HiveDatasetFinder;
import org.apache.gobblin.data.management.copy.hive.HiveTargetPathHelper;
import org.apache.gobblin.hive.HiveMetastoreClientPool;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Table;


public class MockHiveDatasetFinder extends HiveDatasetFinder {

  Path tempDirRoot;
  SourceState state;

  public MockHiveDatasetFinder(LocalFileSystem fs, Properties properties,
      EventSubmitter eventSubmitter) throws IOException {
    super(fs, properties, eventSubmitter);
    this.tempDirRoot = new Path(properties.getProperty("tempDirRoot"));
  }

  public Collection<DbAndTable> getTables() throws IOException {
    List<DbAndTable> tables = Lists.newArrayList();

    for (int i = 0; i < 3; i++) {
      tables.add(new DbAndTable("testDB", "table" + i));
    }

    return tables;
  }

  @Override
  public Iterator<HiveDataset> getDatasetsIterator() throws IOException {

    return new AbstractIterator<HiveDataset>() {
      private Iterator<DbAndTable> tables = getTables().iterator();

      @Override
      protected HiveDataset computeNext() {
        try {
          while (this.tables.hasNext()) {
            DbAndTable dbAndTable = this.tables.next();
            File dbPath = new File(tempDirRoot + "/testPath/testDB/" + dbAndTable.getTable());
            fs.mkdirs(new Path(dbPath.getAbsolutePath()));
            Properties hiveProperties = new Properties();
            hiveProperties.setProperty(ConfigurationKeys.USE_DATASET_LOCAL_WORK_DIR, "true");
            hiveProperties.setProperty(HiveTargetPathHelper.COPY_TARGET_TABLE_PREFIX_TOBE_REPLACED, tempDirRoot + "/testPath");
            hiveProperties.setProperty(HiveTargetPathHelper.COPY_TARGET_TABLE_PREFIX_REPLACEMENT, tempDirRoot + "/targetPath");
            Table table = new Table(Table.getEmptyTable(dbAndTable.getDb(), dbAndTable.getTable()));
            table.setDataLocation(new Path(dbPath.getPath()));
            HiveMetastoreClientPool pool = HiveMetastoreClientPool.get(new Properties(), Optional.absent());
            HiveDataset dataset = new HiveDataset(new LocalFileSystem(), pool, table, hiveProperties);
            return dataset;
          }
        } catch (IOException e) {
          Throwables.propagate(e);
        }
        return endOfData();
      }
    };
  }
}

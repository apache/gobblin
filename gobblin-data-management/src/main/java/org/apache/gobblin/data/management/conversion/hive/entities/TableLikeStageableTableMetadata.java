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

package org.apache.gobblin.data.management.conversion.hive.entities;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.gobblin.data.management.copy.hive.HiveDataset;
import org.apache.hadoop.hive.ql.metadata.Table;

import com.google.common.base.Optional;
import com.typesafe.config.Config;


/**
 * A {@link StageableTableMetadata} that copies most metadata from a reference table.
 */
public class TableLikeStageableTableMetadata extends StageableTableMetadata {

  public TableLikeStageableTableMetadata(Table referenceTable, String destinationDB, String destinationTableName, String targetDataPath) {
    super(destinationTableName, destinationTableName + "_STAGING", destinationDB, targetDataPath,
        getTableProperties(referenceTable), new ArrayList<>(), Optional.of(referenceTable.getNumBuckets()), new Properties(), false, Optional.absent(),
        new ArrayList<>());
  }

  public TableLikeStageableTableMetadata(Table referenceTable, Config config) {
    super(HiveDataset.resolveTemplate(config.getString(StageableTableMetadata.DESTINATION_TABLE_KEY), referenceTable),
        HiveDataset.resolveTemplate(config.getString(StageableTableMetadata.DESTINATION_TABLE_KEY), referenceTable) + "_STAGING",
        HiveDataset.resolveTemplate(config.getString(StageableTableMetadata.DESTINATION_DB_KEY), referenceTable),
        HiveDataset.resolveTemplate(config.getString(DESTINATION_DATA_PATH_KEY), referenceTable),
        getTableProperties(referenceTable), new ArrayList<>(), Optional.of(referenceTable.getNumBuckets()),
        new Properties(), false, Optional.absent(), new ArrayList<>());
  }

  private static Properties getTableProperties(Table table) {
    Properties properties = new Properties();
    properties.putAll(table.getParameters());
    return properties;
  }
}

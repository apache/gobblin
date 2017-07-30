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
package gobblin.data.management.conversion.hive.writer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Optional;

import lombok.AllArgsConstructor;

import gobblin.configuration.State;
import gobblin.data.management.conversion.hive.dataset.ConvertibleHiveDataset;
import gobblin.data.management.conversion.hive.entities.QueryBasedHiveConversionEntity;
import gobblin.data.management.conversion.hive.entities.SchemaAwareHivePartition;
import gobblin.data.management.conversion.hive.events.EventWorkunitUtils;
import gobblin.data.management.conversion.hive.publisher.HiveConvertPublisher;
import gobblin.util.HiveJdbcConnector;
import gobblin.writer.DataWriter;
import lombok.extern.slf4j.Slf4j;


/**
 * The {@link HiveQueryExecutionWriter} is responsible for running the hive query available at
 * {@link QueryBasedHiveConversionEntity#getConversionQuery()}
 */
@Slf4j
@AllArgsConstructor
public class HiveQueryExecutionWriter implements DataWriter<QueryBasedHiveConversionEntity> {

  private final HiveJdbcConnector hiveJdbcConnector;
  private final State workUnit;
  private static final String AT_CHAR = "@";

  @Override
  public void write(QueryBasedHiveConversionEntity hiveConversionEntity) throws IOException {
    List<String> conversionQueries = null;
    try {
      conversionQueries = hiveConversionEntity.getQueries();
      EventWorkunitUtils.setBeginConversionDDLExecuteTimeMetadata(this.workUnit, System.currentTimeMillis());
      this.hiveJdbcConnector.executeStatements(conversionQueries.toArray(new String[conversionQueries.size()]));
      // Adding properties for preserving partitionParams:
      addPropsForPublisher(hiveConversionEntity);
      EventWorkunitUtils.setEndConversionDDLExecuteTimeMetadata(this.workUnit, System.currentTimeMillis());
    } catch (SQLException e) {
      log.warn("Failed to execute queries: ");
      for (String conversionQuery : conversionQueries) {
        log.warn("Conversion query attempted by Hive Query writer: " + conversionQuery);
      }
      throw new IOException(e);
    }
  }

  /**
   * Method to add properties needed by publisher to preserve partition params
   */
  private void addPropsForPublisher(QueryBasedHiveConversionEntity hiveConversionEntity) {
    if (!hiveConversionEntity.getHivePartition().isPresent()) {
      return;
    }
    ConvertibleHiveDataset convertibleHiveDataset = hiveConversionEntity.getConvertibleHiveDataset();
    for (String format : convertibleHiveDataset.getDestFormats()) {
      Optional<ConvertibleHiveDataset.ConversionConfig> conversionConfigForFormat =
          convertibleHiveDataset.getConversionConfigForFormat(format);
      if (!conversionConfigForFormat.isPresent()) {
        continue;
      }
      SchemaAwareHivePartition sourcePartition = hiveConversionEntity.getHivePartition().get();

      // Get complete source partition name dbName@tableName@partitionName
      String completeSourcePartitionName = StringUtils.join(Arrays
          .asList(sourcePartition.getTable().getDbName(), sourcePartition.getTable().getTableName(),
              sourcePartition.getName()), AT_CHAR);
      ConvertibleHiveDataset.ConversionConfig config = conversionConfigForFormat.get();

      // Get complete destination partition name dbName@tableName@partitionName
      String completeDestPartitionName = StringUtils.join(
          Arrays.asList(config.getDestinationDbName(), config.getDestinationTableName(), sourcePartition.getName()),
          AT_CHAR);

      workUnit.setProp(HiveConvertPublisher.COMPLETE_SOURCE_PARTITION_NAME, completeSourcePartitionName);
      workUnit.setProp(HiveConvertPublisher.COMPLETE_DEST_PARTITION_NAME, completeDestPartitionName);
    }
  }

  @Override
  public void commit() throws IOException {}

  @Override
  public void close() throws IOException {
    this.hiveJdbcConnector.close();
  }

  @Override
  public void cleanup() throws IOException {}

  @Override
  public long recordsWritten() {
    return 0;
  }

  @Override
  public long bytesWritten() throws IOException {
    return 0;
  }
}

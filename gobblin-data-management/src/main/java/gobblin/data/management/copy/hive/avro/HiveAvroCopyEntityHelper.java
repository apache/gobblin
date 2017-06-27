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

package gobblin.data.management.copy.hive.avro;

import java.io.IOException;
import java.net.URI;

import com.google.common.base.Optional;

import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.Partition;

import gobblin.data.management.copy.hive.HiveCopyEntityHelper;
import gobblin.util.PathUtils;


/**
 * Update avro related entries in creating {@link gobblin.data.management.copy.CopyEntity}s for copying a Hive table.
 */

@Slf4j
public class HiveAvroCopyEntityHelper {
  private static final String HIVE_TABLE_AVRO_SCHEMA_URL  = "avro.schema.url";

  /**
   * Currently updated the {@link #HIVE_TABLE_AVRO_SCHEMA_URL} location for new hive table
   * @param targetTable, new Table to be registered in hive
   * @throws IOException
   */
  public static void updateTableAttributesIfAvro(Table targetTable, HiveCopyEntityHelper hiveHelper) throws IOException {
    if (isHiveTableAvroType(targetTable)) {
      updateAvroSchemaURL(targetTable.getCompleteName(), targetTable.getTTable().getSd(), hiveHelper);
    }
  }

  /**
   * Currently updated the {@link #HIVE_TABLE_AVRO_SCHEMA_URL} location for new hive partitions
   * @param targetTable, new Table to be registered in hive
   * @param sourcePartitions, source partitions
   * @throws IOException
   */
  public static void updatePartitionAttributesIfAvro(Table targetTable, Map<List<String>, Partition> sourcePartitions, HiveCopyEntityHelper hiveHelper) throws IOException {
    if (isHiveTableAvroType(targetTable)) {
      for (Map.Entry<List<String>, Partition> partition : sourcePartitions.entrySet()) {
        updateAvroSchemaURL(partition.getValue().getCompleteName(), partition.getValue().getTPartition().getSd(), hiveHelper);
      }
    }
  }

  /**
   *
   * @param entity, name of the entity to be changed, e.g. hive table or partition
   * @param sd, StorageDescriptor of the entity
   */
  public static void updateAvroSchemaURL(String entity, StorageDescriptor sd, HiveCopyEntityHelper hiveHelper) {
    String oldAvroSchemaURL = sd.getSerdeInfo().getParameters().get(HIVE_TABLE_AVRO_SCHEMA_URL);
    if (oldAvroSchemaURL != null) {

      Path oldAvroSchemaPath = new Path(oldAvroSchemaURL);
      URI sourceFileSystemURI = hiveHelper.getDataset().getFs().getUri();

      if (PathUtils.isAbsoluteAndSchemeAuthorityNull(oldAvroSchemaPath)
          || (oldAvroSchemaPath.toUri().getScheme().equals(sourceFileSystemURI.getScheme())
          && oldAvroSchemaPath.toUri().getAuthority().equals(sourceFileSystemURI.getAuthority()))) {

        String newAvroSchemaURL = hiveHelper.getTargetPathHelper().getTargetPath(oldAvroSchemaPath, hiveHelper.getTargetFileSystem(),
            Optional.<Partition>absent(), true).toString();

        sd.getSerdeInfo().getParameters().put(HIVE_TABLE_AVRO_SCHEMA_URL, newAvroSchemaURL);
        log.info(String.format("For entity %s, change %s from %s to %s", entity,
            HIVE_TABLE_AVRO_SCHEMA_URL, oldAvroSchemaURL, newAvroSchemaURL));
      }
    }
  }

  /**
   * Tell whether a hive table is actually an Avro table
   * @param targetTable
   * @return
   * @throws IOException
   */
  public static boolean isHiveTableAvroType(Table targetTable) throws IOException {
    String serializationLib = targetTable.getTTable().getSd().getSerdeInfo().getSerializationLib();
    String inputFormat = targetTable.getTTable().getSd().getInputFormat();
    String outputFormat = targetTable.getTTable().getSd().getOutputFormat();

    return inputFormat.endsWith("AvroContainerInputFormat") || outputFormat.endsWith("AvroContainerOutputFormat")
        || serializationLib.endsWith("AvroSerDe");
  }
}

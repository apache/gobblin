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

package org.apache.gobblin.data.management.copy.iceberg;

import java.io.IOException;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;

import lombok.extern.slf4j.Slf4j;

/**
 * Validator for Iceberg table metadata, ensuring that the given tables metadata have same schema and partition spec.
 */
@Slf4j
public class IcebergTableMetadataValidatorUtils {

  private IcebergTableMetadataValidatorUtils() {
    // Do not instantiate
  }

  /**
   * Compares the metadata of the given two iceberg tables.
   * <ul>
   *   <li>First compares the schema of the metadata.</li>
   *   <li>Then compares the partition spec of the metadata.</li>
   * </ul>
   * @param tableAMetadata  the metadata of the first table
   * @param tableBMetadata the metadata of the second table
   * @throws IOException if the schemas or partition spec do not match
   */
  public static void failUnlessCompatibleStructure(TableMetadata tableAMetadata,
      TableMetadata tableBMetadata) throws IOException {
    log.info("Starting comparison between iceberg tables with metadata file location : {} and {}",
        tableAMetadata.metadataFileLocation(),
        tableBMetadata.metadataFileLocation());

    Schema tableASchema = tableAMetadata.schema();
    Schema tableBSchema = tableBMetadata.schema();
    // TODO: Need to add support for schema evolution
    //  This check needs to be broken down into multiple checks to support schema evolution
    //  Possible cases - tableASchema == tableBSchema,
    //  - tableASchema is subset of tableBSchema [ tableBSchema Evolved ],
    //  - tableASchema is superset of tableBSchema [ tableASchema Evolved ],
    //  - Other cases?
    //  Also consider using Strategy or any other design pattern for this to make it a better solution
    if (!tableASchema.sameSchema(tableBSchema)) {
      String errMsg = String.format(
          "Schema Mismatch between Metadata{%s} - SchemaId{%d} and Metadata{%s} - SchemaId{%d}",
          tableAMetadata.metadataFileLocation(),
          tableASchema.schemaId(),
          tableBMetadata.metadataFileLocation(),
          tableBSchema.schemaId()
      );
      log.error(errMsg);
      throw new IOException(errMsg);
    }

    PartitionSpec tableAPartitionSpec = tableAMetadata.spec();
    PartitionSpec tableBPartitionSpec = tableBMetadata.spec();
    if (!tableAPartitionSpec.compatibleWith(tableBPartitionSpec)) {
      String errMsg = String.format(
          "Partition Spec Mismatch between Metadata{%s} - PartitionSpecId{%d} and Metadata{%s} - PartitionSpecId{%d}",
          tableAMetadata.metadataFileLocation(),
          tableAPartitionSpec.specId(),
          tableBMetadata.metadataFileLocation(),
          tableBPartitionSpec.specId()
      );
      log.error(errMsg);
      throw new IOException(errMsg);
    }

    log.info("Comparison completed successfully between iceberg tables with metadata file location : {} and {}",
        tableAMetadata.metadataFileLocation(),
        tableBMetadata.metadataFileLocation());
  }
}

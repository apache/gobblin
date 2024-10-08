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
 * Validator for Iceberg table metadata, ensuring that the source and destination tables have
 * compatible schemas and partition specifications.
 */
@Slf4j
public class IcebergTableMetadataValidatorUtils {

  private IcebergTableMetadataValidatorUtils() {
    // Do not instantiate
  }

  /**
   * Validates the metadata of the source and destination Iceberg tables.
   *
   * @param srcTableMetadata  the metadata of the source table
   * @param destTableMetadata the metadata of the destination table
   * @throws IOException if the schemas or partition specifications do not match
   */
  public static void validateSourceAndDestinationTablesMetadata(TableMetadata srcTableMetadata,
      TableMetadata destTableMetadata) throws IOException {
    log.info("Starting validation of Source : {} and Destination : {} Iceberg Tables Metadata",
        srcTableMetadata.location(),
        destTableMetadata.location());
    Schema srcTableSchema = srcTableMetadata.schema();
    Schema destTableSchema = destTableMetadata.schema();
    PartitionSpec srcPartitionSpec = srcTableMetadata.spec();
    PartitionSpec destPartitionSpec = destTableMetadata.spec();
    validateSchemaForEquality(srcTableSchema, destTableSchema);
    validatePartitionSpecForEquality(srcPartitionSpec, destPartitionSpec);
    log.info("Validation of Source : {} and Destination : {} Iceberg Tables Metadata completed successfully",
        srcTableMetadata.location(),
        destTableMetadata.location());
  }

  private static void validateSchemaForEquality(Schema srcTableSchema, Schema destTableSchema) throws IOException {
    // TODO: Need to add support for schema evolution, currently only supporting copying
    //  between iceberg tables with same schema.
    //  This function needs to be broken down into multiple functions to support schema evolution
    //  Possible cases - Src Schema == Dest Schema,
    //  - Src Schema is subset of Dest Schema [ Destination Schema Evolved ],
    //  - Src Schema is superset of Dest Schema [ Source Schema Evolved ],
    //  - Other cases?
    //  Also consider using Strategy or any other design pattern for this to make it a better solution
    if (!srcTableSchema.sameSchema(destTableSchema)) {
      String errMsg = String.format(
          "Schema Mismatch between Source and Destination Iceberg Tables Schema - Source-Schema-Id : {%s} and "
              + "Destination-Schema-Id : {%s}",
          srcTableSchema.schemaId(),
          destTableSchema.schemaId()
      );
      log.error(errMsg);
      throw new IOException(errMsg);
    }
  }

  private static void validatePartitionSpecForEquality(PartitionSpec srcPartitionSpec, PartitionSpec destPartitionSpec)
      throws IOException {
    // Currently, only supporting copying between iceberg tables with same partition spec
    if (!srcPartitionSpec.compatibleWith(destPartitionSpec)) {
      String errMsg = String.format(
          "Partition Spec Mismatch between Source and Destination Iceberg Tables Partition Spec - Source : {%s} and Destination : {%s}",
          srcPartitionSpec,
          destPartitionSpec
      );
      log.error(errMsg);
      throw new IOException(errMsg);
    }
  }
}

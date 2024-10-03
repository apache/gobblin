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

import java.util.Arrays;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;

import com.google.common.annotations.VisibleForTesting;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IcebergTableMetadataValidator {

  public static void validateSourceAndDestinationTablesMetadata(TableMetadata srcTableMetadata, TableMetadata destTableMetadata) {
    log.info("Starting validation of Source and Destination Iceberg Tables Metadata");
    Schema srcTableSchema = srcTableMetadata.schema();
    Schema destTableSchema = destTableMetadata.schema();
    PartitionSpec srcPartitionSpec = srcTableMetadata.spec();
    PartitionSpec destPartitionSpec = destTableMetadata.spec();
    validateSchema(srcTableSchema, destTableSchema);
    validatePartitionSpec(srcPartitionSpec, destPartitionSpec);
    log.info("Validation of Source and Destination Iceberg Tables Metadata completed successfully");
  }

  @VisibleForTesting
  protected static void validateSchema(Schema srcTableSchema, Schema destTableSchema) {
    // TODO: Need to add support for schema evolution, currently only supporting copying
    //  between iceberg tables with same schema.
    //  This function needs to be broken down into multiple functions to support schema evolution
    //  Possible cases - Src Schema == Dest Schema,
    //  - Src Schema is subset of Dest Schema [ Destination Schema Evolved ],
    //  - Src Schema is superset of Dest Schema [ Source Schema Evolved ],
    //  - Other cases?
    if (!srcTableSchema.sameSchema(destTableSchema)) {
      String errMsg = String.format(
          "Schema Mismatch between Source and Destination Iceberg Tables Schema - Source : {%s} and Destination : {%s}",
          srcTableSchema,
          destTableSchema
      );
      log.error(errMsg);
      throw new IllegalArgumentException(errMsg);
    }
  }

  @VisibleForTesting
  protected static void validatePartitionSpec(PartitionSpec srcPartitionSpec, PartitionSpec destPartitionSpec) {
    // Currently, only supporting copying between iceberg tables with same partition spec
    if (!srcPartitionSpec.compatibleWith(destPartitionSpec)) {
      String errMsg = String.format(
          "Partition Spec Mismatch between Source and Destination Iceberg Tables Partition Spec - Source : {%s} and Destination : {%s}",
          srcPartitionSpec,
          destPartitionSpec
      );
      log.error(errMsg);
      throw new IllegalArgumentException(errMsg);
    }
    // .compatibleWith() does not check if the partition field in partition spec have same java classes or not
    // i.e. if the partition field in partition spec is of type Integer in src table and String in dest table,
    // so need to put an additional check for that
    // try to run test testValidatePartitionSpecWithDiffType() in IcebergTableMetadataValidatorTest.java with
    // this check commented out
    // TODO: This check can be removed after adding support for schema evolution
    if (!Arrays.equals(srcPartitionSpec.javaClasses(), destPartitionSpec.javaClasses())) {
      String errMsg = String.format(
          "Partition Spec Have different types for same partition field between Source and Destination Iceberg Table - "
              + "Source : {%s} and Destination : {%s}",
          srcPartitionSpec,
          destPartitionSpec
      );
      log.error(errMsg);
      throw new IllegalArgumentException(errMsg);
    }
  }

}

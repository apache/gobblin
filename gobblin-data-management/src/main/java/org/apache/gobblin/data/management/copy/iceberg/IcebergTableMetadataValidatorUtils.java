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
   * @param tableMetadataA  the metadata of the first table
   * @param tableMetadataB the metadata of the second table
   * @param validateStrictPartitionEquality boolean value to control strictness of partition spec comparison
   * @throws IOException if the schemas or partition spec do not match
   */
  public static void failUnlessCompatibleStructure(TableMetadata tableMetadataA,
      TableMetadata tableMetadataB, boolean validateStrictPartitionEquality) throws IOException {
    log.info("Starting comparison between iceberg tables with metadata file location : {} and {}",
        tableMetadataA.metadataFileLocation(),
        tableMetadataB.metadataFileLocation());

    PartitionSpec partitionSpecA = tableMetadataA.spec();
    PartitionSpec partitionSpecB = tableMetadataB.spec();
    // .compatibleWith() doesn't match for specId of partition spec and fieldId of partition fields while .equals() does
    boolean partitionSpecMatch = validateStrictPartitionEquality ? partitionSpecA.equals(partitionSpecB)
        : partitionSpecA.compatibleWith(partitionSpecB);
    if (!partitionSpecMatch) {
      String errMsg = String.format(
          "Partition Spec Mismatch between Metadata{%s} - PartitionSpecId{%d} and Metadata{%s} - PartitionSpecId{%d}",
          tableMetadataA.metadataFileLocation(),
          partitionSpecA.specId(),
          tableMetadataB.metadataFileLocation(),
          partitionSpecB.specId()
      );
      log.error(errMsg);
      throw new IOException(errMsg);
    }

    log.info("Comparison completed successfully between iceberg tables with metadata file location : {} and {}",
        tableMetadataA.metadataFileLocation(),
        tableMetadataB.metadataFileLocation());
  }
}

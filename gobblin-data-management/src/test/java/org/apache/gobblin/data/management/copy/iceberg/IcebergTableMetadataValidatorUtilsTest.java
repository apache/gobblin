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
import java.util.HashMap;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.shaded.org.apache.avro.SchemaBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

public class IcebergTableMetadataValidatorUtilsTest {
  private static final PartitionSpec unpartitionedPartitionSpec = PartitionSpec.unpartitioned();
  private static final Schema schema1 = AvroSchemaUtil.toIceberg(SchemaBuilder.record("schema1")
      .fields()
      .requiredString("field1")
      .requiredString("field2")
      .endRecord());
  private static final PartitionSpec partitionSpec1 = PartitionSpec.builderFor(schema1)
      .identity("field1")
      .build();
  private static final TableMetadata tableMetadataWithSchema1AndUnpartitionedSpec = TableMetadata.newTableMetadata(
      schema1, unpartitionedPartitionSpec, "tableLocationForSchema1WithUnpartitionedSpec", new HashMap<>());
  private static final TableMetadata tableMetadataWithSchema1AndPartitionSpec1 = TableMetadata.newTableMetadata(
      schema1, partitionSpec1, "tableLocationForSchema1WithPartitionSpec1", new HashMap<>());
  private static final String PARTITION_SPEC_MISMATCH_EXCEPTION = "Partition Spec Mismatch between Metadata";
  private static final boolean VALIDATE_STRICT_PARTITION_EQUALITY_TRUE = true;
  private static final boolean VALIDATE_STRICT_PARTITION_EQUALITY_FALSE = false;
  @Test
  public void testValidateSameSchema() throws IOException {
    IcebergTableMetadataValidatorUtils.failUnlessCompatibleStructure(
        tableMetadataWithSchema1AndUnpartitionedSpec, tableMetadataWithSchema1AndUnpartitionedSpec,
        VALIDATE_STRICT_PARTITION_EQUALITY_TRUE
    );
    Assert.assertTrue(true);
  }

  @Test
  public void testValidateSamePartitionSpec() throws IOException {
    IcebergTableMetadataValidatorUtils.failUnlessCompatibleStructure(
        tableMetadataWithSchema1AndPartitionSpec1, tableMetadataWithSchema1AndPartitionSpec1,
        VALIDATE_STRICT_PARTITION_EQUALITY_TRUE
    );
    Assert.assertTrue(true);
  }

  @Test
  public void testValidatePartitionSpecWithDiffNameFails() {
    PartitionSpec partitionSpec12 = PartitionSpec.builderFor(schema1)
        .identity("field2")
        .build();
    TableMetadata tableMetadataWithSchema1AndPartitionSpec12 = TableMetadata.newTableMetadata(schema1, partitionSpec12,
        "tableLocationForSchema1WithPartitionSpec12", new HashMap<>());
    verifyStrictFailUnlessCompatibleStructureThrows(tableMetadataWithSchema1AndPartitionSpec1,
        tableMetadataWithSchema1AndPartitionSpec12, PARTITION_SPEC_MISMATCH_EXCEPTION);
  }

  @Test
  public void testValidatePartitionSpecWithUnpartitionedFails() {
    verifyStrictFailUnlessCompatibleStructureThrows(tableMetadataWithSchema1AndUnpartitionedSpec,
        tableMetadataWithSchema1AndPartitionSpec1, PARTITION_SPEC_MISMATCH_EXCEPTION);
  }

  @Test
  public void testPartitionSpecWithDifferentTransformFails() {
    PartitionSpec partitionSpec12 = PartitionSpec.builderFor(schema1)
        .truncate("field1", 4)
        .build();
    TableMetadata tableMetadataWithSchema1AndPartitionSpec12 = TableMetadata.newTableMetadata(schema1, partitionSpec12,
        "tableLocationForSchema1WithPartitionSpec12", new HashMap<>());
    verifyStrictFailUnlessCompatibleStructureThrows(tableMetadataWithSchema1AndPartitionSpec1,
        tableMetadataWithSchema1AndPartitionSpec12, PARTITION_SPEC_MISMATCH_EXCEPTION);
  }

  @Test
  public void testStrictPartitionSpecEqualityOffVsOn() throws IOException {
    PartitionSpec partitionSpecWithTwoCols = PartitionSpec.builderFor(schema1)
        .identity("field1")
        .identity("field2")
        .build();

    TableMetadata tableMetadataWithSchema1AndPartitionSpecWithTwoCols = TableMetadata.newTableMetadata(schema1,
        partitionSpecWithTwoCols, "tableLocationForSchema1WithPartitionSpecWithTwoCols", new HashMap<>());
    TableMetadata updatedMetadataForTableMetadataWithSchema1AndPartitionSpec1 = tableMetadataWithSchema1AndPartitionSpec1
        .updatePartitionSpec(tableMetadataWithSchema1AndPartitionSpecWithTwoCols.spec());

    IcebergTableMetadataValidatorUtils.failUnlessCompatibleStructure(
        tableMetadataWithSchema1AndPartitionSpecWithTwoCols,
        updatedMetadataForTableMetadataWithSchema1AndPartitionSpec1,
        VALIDATE_STRICT_PARTITION_EQUALITY_FALSE);
    Assert.assertTrue(true); // passes w/ non-strict equality...
    // but fails when strict equality
    verifyStrictFailUnlessCompatibleStructureThrows(tableMetadataWithSchema1AndPartitionSpecWithTwoCols,
        updatedMetadataForTableMetadataWithSchema1AndPartitionSpec1, PARTITION_SPEC_MISMATCH_EXCEPTION);
  }

  private void verifyStrictFailUnlessCompatibleStructureThrows(TableMetadata tableAMetadata,
      TableMetadata tableBMetadata, String expectedMessage) {
    IOException exception = Assert.expectThrows(IOException.class, () -> {
      IcebergTableMetadataValidatorUtils.failUnlessCompatibleStructure(tableAMetadata, tableBMetadata,
          VALIDATE_STRICT_PARTITION_EQUALITY_TRUE);
    });
    Assert.assertTrue(exception.getMessage().startsWith(expectedMessage));
  }
}

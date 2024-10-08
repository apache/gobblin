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
  private static final org.apache.iceberg.shaded.org.apache.avro.Schema avroDataSchema1 =
      SchemaBuilder.record("schema1")
          .fields()
          .requiredString("field1")
          .requiredString("field2")
          .endRecord();
  private static final org.apache.iceberg.shaded.org.apache.avro.Schema avroDataSchema2 =
      SchemaBuilder.record("schema2")
          .fields()
          .requiredString("field2")
          .requiredString("field1")
          .endRecord();
  private static final org.apache.iceberg.shaded.org.apache.avro.Schema avroDataSchema3 =
      SchemaBuilder.record("schema3")
          .fields()
          .requiredString("field1")
          .requiredString("field2")
          .requiredInt("field3")
          .endRecord();
  private static final org.apache.iceberg.shaded.org.apache.avro.Schema avroDataSchema4 =
      SchemaBuilder.record("schema4")
          .fields()
          .requiredInt("field1")
          .requiredString("field2")
          .requiredInt("field3")
          .endRecord();
  private static final PartitionSpec unpartitionedPartitionSpec = PartitionSpec.unpartitioned();
  private static final Schema schema1 = AvroSchemaUtil.toIceberg(avroDataSchema1);
  private static final Schema schema2IsNotSchema1Compat = AvroSchemaUtil.toIceberg(avroDataSchema2);
  private static final Schema schema3 = AvroSchemaUtil.toIceberg(avroDataSchema3);
  private static final Schema schema4IsNotSchema3Compat = AvroSchemaUtil.toIceberg(avroDataSchema4);
  private static final PartitionSpec partitionSpec1 = PartitionSpec.builderFor(schema1)
      .identity("field1")
      .build();
  private static final TableMetadata tableMetadataWithSchema1AndUnpartitionedSpec = TableMetadata.newTableMetadata(
      schema1, unpartitionedPartitionSpec, "metadataLocationWithSchema1AndUnpartitionedSpec", new HashMap<>());
  private static final TableMetadata tableMetadataWithSchema1AndPartitionSpec1 = TableMetadata.newTableMetadata(
      schema1, partitionSpec1, "metadataLocationWithSchema2AndUnpartitionedSpec", new HashMap<>());
  private static final TableMetadata tableMetadataWithSchema3AndUnpartitionedSpec = TableMetadata.newTableMetadata(
      schema3, unpartitionedPartitionSpec, "metadataLocationWithSchema1AndUnpartitionedSpec", new HashMap<>());
  private static final String SCHEMA_MISMATCH_EXCEPTION = "Schema Mismatch between Source and Destination Iceberg Tables Schema";
  private static final String PARTITION_SPEC_MISMATCH_EXCEPTION = "Partition Spec Mismatch between Source and Destination Iceberg Tables Partition Spec";
  @Test
  public void testValidateSameSchema() throws IOException {
    IcebergTableMetadataValidatorUtils.validateSourceAndDestinationTablesMetadata(
        tableMetadataWithSchema1AndUnpartitionedSpec, tableMetadataWithSchema1AndUnpartitionedSpec
    );
  }

  @Test
  public void testValidateDifferentSchema() {
    // Schema 1 and Schema 2 have different field order

    TableMetadata destTableMetadata = TableMetadata.newTableMetadata(schema2IsNotSchema1Compat,
        unpartitionedPartitionSpec, "destMetadataLocationForSchema2", new HashMap<>());

    verifyValidateSourceAndDestinationTablesMetadataIOException(tableMetadataWithSchema1AndUnpartitionedSpec,
        destTableMetadata, SCHEMA_MISMATCH_EXCEPTION);
  }

  @Test
  public void testValidateSchemaWithDifferentTypes() {
    // schema 3 and schema 4 have different field types for field1

    TableMetadata destTableMetadata = TableMetadata.newTableMetadata(schema4IsNotSchema3Compat,
        unpartitionedPartitionSpec, "destMetadataLocationForSchema4", new HashMap<>());

    verifyValidateSourceAndDestinationTablesMetadataIOException(tableMetadataWithSchema3AndUnpartitionedSpec,
        destTableMetadata, SCHEMA_MISMATCH_EXCEPTION);
  }

  @Test
  public void testValidateSchemaWithEvolvedDestinationSchema() {
    // TODO: This test should pass in the future when we support source side schema evolution and here there should
    //  not be any commit needed on destination side
    // Schema 3 has one more extra field as compared to Schema 1
    verifyValidateSourceAndDestinationTablesMetadataIOException(tableMetadataWithSchema1AndUnpartitionedSpec,
        tableMetadataWithSchema3AndUnpartitionedSpec, SCHEMA_MISMATCH_EXCEPTION);
  }

  @Test
  public void testValidateSchemaWithEvolvedSourceSchema() {
    // TODO: This test should pass in the future when we support source side schema evolution and commit the changes
    //  on destination side either through IcebergRegisterStep or any other CommitStep
    // Schema 3 has one more extra field as compared to Schema 1
    verifyValidateSourceAndDestinationTablesMetadataIOException(tableMetadataWithSchema3AndUnpartitionedSpec,
        tableMetadataWithSchema1AndUnpartitionedSpec, SCHEMA_MISMATCH_EXCEPTION);
  }

  @Test
  public void testValidateEvolvedSourceSchemaFromIntToLongType() {
    // Adding this test as to verify that partition copy doesn't proceed further for this case
    // as while doing poc and testing had seen final commit gets fail if there is mismatch in field type
    // specially from int to long
    org.apache.iceberg.shaded.org.apache.avro.Schema avroDataSchema =
        SchemaBuilder.record("schema5")
            .fields()
            .requiredLong("field1")
            .requiredString("field2")
            .requiredInt("field3")
            .endRecord();
    Schema schema5EvolvedFromSchema4 = AvroSchemaUtil.toIceberg(avroDataSchema);
    PartitionSpec partitionSpec = PartitionSpec.builderFor(schema5EvolvedFromSchema4)
        .identity("field1")
        .build();
    TableMetadata destTableMetadata = TableMetadata.newTableMetadata(schema5EvolvedFromSchema4, partitionSpec,
        "destMetadataLocationForSchema5", new HashMap<>());

    verifyValidateSourceAndDestinationTablesMetadataIOException(tableMetadataWithSchema1AndUnpartitionedSpec,
        destTableMetadata, SCHEMA_MISMATCH_EXCEPTION);
  }

  @Test
  public void testValidateSamePartitionSpec() throws IOException {
    IcebergTableMetadataValidatorUtils.validateSourceAndDestinationTablesMetadata(
        tableMetadataWithSchema1AndPartitionSpec1, tableMetadataWithSchema1AndPartitionSpec1
    );
  }

  @Test
  public void testValidatePartitionSpecWithDiffName() {
    PartitionSpec partitionSpec12 = PartitionSpec.builderFor(schema1)
        .identity("field2")
        .build();
    TableMetadata destTableMetadata = TableMetadata.newTableMetadata(schema1, partitionSpec12,
        "destMetadataLocationForSchema1", new HashMap<>());
    verifyValidateSourceAndDestinationTablesMetadataIOException(tableMetadataWithSchema1AndPartitionSpec1,
        destTableMetadata, PARTITION_SPEC_MISMATCH_EXCEPTION);
  }

  @Test
  public void testValidatePartitionSpecWithUnpartitioned() {
    verifyValidateSourceAndDestinationTablesMetadataIOException(tableMetadataWithSchema1AndUnpartitionedSpec,
        tableMetadataWithSchema1AndPartitionSpec1, PARTITION_SPEC_MISMATCH_EXCEPTION);
  }

  @Test
  public void testPartitionSpecWithDifferentTransform() {
    PartitionSpec partitionSpec12 = PartitionSpec.builderFor(schema1)
        .truncate("field1", 4)
        .build();
    TableMetadata destTableMetadata = TableMetadata.newTableMetadata(schema1, partitionSpec12,
        "destMetadataLocationForSchema1", new HashMap<>());
    verifyValidateSourceAndDestinationTablesMetadataIOException(tableMetadataWithSchema1AndPartitionSpec1,
        destTableMetadata, PARTITION_SPEC_MISMATCH_EXCEPTION);
  }

  private void verifyValidateSourceAndDestinationTablesMetadataIOException(TableMetadata srcTableMetadata,
      TableMetadata destTableMetadata, String expectedMessage) {
    IOException exception = Assert.expectThrows(IOException.class, () -> {
      IcebergTableMetadataValidatorUtils.validateSourceAndDestinationTablesMetadata(srcTableMetadata, destTableMetadata);
    });
    Assert.assertTrue(exception.getMessage().startsWith(expectedMessage));
  }
}

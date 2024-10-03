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

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.shaded.org.apache.avro.SchemaBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

public class IcebergTableMetadataValidatorTest {
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
      SchemaBuilder.record("schema3")
          .fields()
          .requiredInt("field1")
          .requiredString("field2")
          .requiredInt("field3")
          .endRecord();
  private static final Schema schema1 = AvroSchemaUtil.toIceberg(avroDataSchema1);
  private static final Schema schema2 = AvroSchemaUtil.toIceberg(avroDataSchema2);
  private static final Schema schema3 = AvroSchemaUtil.toIceberg(avroDataSchema3);
  private static final Schema schema4 = AvroSchemaUtil.toIceberg(avroDataSchema4);
  private static final PartitionSpec partitionSpec1 = PartitionSpec.builderFor(schema1)
      .identity("field1")
      .build();
  private static final String SHOULD_NOT_THROW_EXCEPTION = "Should not throw any exception";
  private static final String SCHEMA_MISMATCH_EXCEPTION = "Schema Mismatch between Source and Destination Iceberg Tables Schema";
  private static final String PARTITION_SPEC_MISMATCH_EXCEPTION = "Partition Spec Mismatch between Source and Destination Iceberg Tables Partition Spec";
  private static final String PARTITION_SPEC_DIFF_TYPE_EXCEPTION = "Partition Spec Have different types for same partition field between Source and Destination Iceberg Table";

  @Test
  public void testValidateSameSchema() {
    try {
      IcebergTableMetadataValidator.validateSchema(schema1, schema1);
    } catch (Exception e) {
      System.out.println(e.getMessage());
      Assert.fail(SHOULD_NOT_THROW_EXCEPTION);
    }
  }

  @Test
  public void testValidateDifferentSchema() {
    // Schema 1 and Schema 2 have different field order
    verifyValidateSchemaIllegalArgumentException(schema1, schema2);
  }

  @Test
  public void testValidateSchemaWithDifferentTypes() {
    verifyValidateSchemaIllegalArgumentException(schema3, schema4);
  }

  @Test
  public void testValidateSchemaWithEvolvedDestinationSchema() {
    // TODO: This test should pass in the future when we support source side schema evolution and here there should
    //  not be any commit needed on destination side
    // Schema 3 has one more extra field as compared to Schema 1
    verifyValidateSchemaIllegalArgumentException(schema1, schema3);
  }

  @Test
  public void testValidateSchemaWithEvolvedSourceSchema() {
    // TODO: This test should pass in the future when we support source side schema evolution and commit the changes
    //  on destination side either through IcebergRegisterStep or any other CommitStep
    // Schema 3 has one more extra field as compared to Schema 1
    verifyValidateSchemaIllegalArgumentException(schema3, schema1);
  }

  @Test
  public void testValidateSamePartitionSpec() {
    try {
      IcebergTableMetadataValidator.validatePartitionSpec(partitionSpec1, partitionSpec1);
    } catch (Exception e) {
      System.out.println(e.getMessage());
      Assert.fail(SHOULD_NOT_THROW_EXCEPTION);
    }
  }

  @Test
  public void testValidatePartitionSpecWithDiffType() {
    PartitionSpec partitionSpec4 = PartitionSpec.builderFor(schema4)
        .identity("field1")
        .build();
    verifyValidatePartitionSpecIllegalArgumentException(partitionSpec1, partitionSpec4, PARTITION_SPEC_DIFF_TYPE_EXCEPTION);
  }

  @Test
  public void testValidatePartitionSpecWithDiffName() {
    PartitionSpec partitionSpec2 = PartitionSpec.builderFor(schema2)
        .identity("field2")
        .build();
    verifyValidatePartitionSpecIllegalArgumentException(partitionSpec1, partitionSpec2, PARTITION_SPEC_MISMATCH_EXCEPTION);
  }

  @Test
  public void testValidatePartitionSpecWithDiffNameAndDiffType() {
    PartitionSpec partitionSpec5 = PartitionSpec.builderFor(schema3)
        .identity("field3")
        .build();
    verifyValidatePartitionSpecIllegalArgumentException(partitionSpec1, partitionSpec5, PARTITION_SPEC_MISMATCH_EXCEPTION);
  }

  @Test
  public void testValidatePartitionSpecWithUnpartitioned() {
    PartitionSpec partitionSpec3 = PartitionSpec.unpartitioned();
    verifyValidatePartitionSpecIllegalArgumentException(partitionSpec1, partitionSpec3, PARTITION_SPEC_MISMATCH_EXCEPTION);
  }

  @Test
  public void testPartitionSpecWithDifferentTransform() {
    PartitionSpec partitionSpec = PartitionSpec.builderFor(schema2)
        .truncate("field2", 4)
        .build();
    verifyValidatePartitionSpecIllegalArgumentException(partitionSpec1, partitionSpec, PARTITION_SPEC_MISMATCH_EXCEPTION);
  }

  private void verifyValidateSchemaIllegalArgumentException(Schema srcSchema, Schema destSchema) {
    IllegalArgumentException exception = Assert.expectThrows(IllegalArgumentException.class, () -> {
      IcebergTableMetadataValidator.validateSchema(srcSchema, destSchema);
    });
    Assert.assertTrue(exception.getMessage().startsWith(SCHEMA_MISMATCH_EXCEPTION));
  }

  private void verifyValidatePartitionSpecIllegalArgumentException(PartitionSpec srcPartitionSpec,
      PartitionSpec destPartitionSpec, String expectedMessage) {
    IllegalArgumentException exception = Assert.expectThrows(IllegalArgumentException.class, () -> {
      IcebergTableMetadataValidator.validatePartitionSpec(srcPartitionSpec, destPartitionSpec);
    });
    Assert.assertTrue(exception.getMessage().startsWith(expectedMessage));
  }
}

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

/** Test {@link org.apache.gobblin.data.management.copy.iceberg.IcebergTableMetadataValidator} */
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
  private static final org.apache.iceberg.shaded.org.apache.avro.Schema avroDataSchema5 =
      SchemaBuilder.record("schema1")
          .fields()
          .requiredInt("field3")
          .requiredInt("field4")
          .endRecord();

  private static final Schema schema1 = AvroSchemaUtil.toIceberg(avroDataSchema1);
  private static final Schema schema2 = AvroSchemaUtil.toIceberg(avroDataSchema2);
  private static final Schema schema3 = AvroSchemaUtil.toIceberg(avroDataSchema3);
  private static final Schema schema4 = AvroSchemaUtil.toIceberg(avroDataSchema4);
  private static final Schema schema5 = AvroSchemaUtil.toIceberg(avroDataSchema5);
  private static final PartitionSpec partitionSpec1 = PartitionSpec.builderFor(schema1)
      .identity("field1")
      .build();
  private static final PartitionSpec partitionSpec12 = PartitionSpec.builderFor(schema1)
      .identity("field2")
      .build();
  private static final PartitionSpec partitionSpec2 = PartitionSpec.builderFor(schema2)
      .identity("field2")
      .build();
  private static final PartitionSpec partitionSpec5 = PartitionSpec.builderFor(schema5)
      .identity("field3")
      .build();

  @Test
  public void testValidateSameSchema() {
    try {
      IcebergTableMetadataValidator.validateSchema(schema1, schema1);
    } catch (Exception e) {
      System.out.println(e.getMessage());
      Assert.fail("Should not throw any exception");
    }
  }

  @Test
  public void testValidateDifferentSchema() {
    try {
      IcebergTableMetadataValidator.validateSchema(schema1, schema2);
      Assert.fail("Should throw an Illegal Argument Exception");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("Schema Mismatch between Source and Destination Iceberg Tables Schema"));
    } catch (Exception e) {
      Assert.fail("Should throw an Illegal Argument Exception");
    }
  }

  @Test
  public void testValidateSchemaWithDifferentTypes() {
    try {
      IcebergTableMetadataValidator.validateSchema(schema3, schema4);
      Assert.fail("Should throw an Illegal Argument Exception");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("Schema Mismatch between Source and Destination Iceberg Tables Schema"));
    } catch (Exception e) {
      Assert.fail("Should throw an Illegal Argument Exception");
    }
  }

  @Test
  public void testValidateFieldIds() {
    try {
      IcebergTableMetadataValidator.validateFieldIds(schema1, schema1);
      IcebergTableMetadataValidator.validateFieldIds(schema2, schema2);
      IcebergTableMetadataValidator.validateFieldIds(schema3, schema3);
    } catch (Exception e) {
      System.out.println(e.getMessage());
      Assert.fail("Should not throw any exception");
    }
  }

  @Test
  public void testValidateFieldIdsWithDifferentFieldNames() {
    try {
      IcebergTableMetadataValidator.validateFieldIds(schema1, schema3);
      Assert.fail("Should throw an Illegal Argument Exception");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("Some columns are missing between source and destination Iceberg Tables Schema"));
    } catch (Exception e) {
      Assert.fail("Should throw an Illegal Argument Exception");
    }
  }

  @Test
  public void testValidateFieldIdsWithDifferentFieldNames2() {
    try {
      // Swapped src & dest from above test
      IcebergTableMetadataValidator.validateFieldIds(schema3, schema1);
      Assert.fail("Should throw an Illegal Argument Exception");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("Some columns are missing between source and destination Iceberg Tables Schema"));
    } catch (Exception e) {
      Assert.fail("Should throw an Illegal Argument Exception");
    }
  }

  @Test
  public void testValidateFieldIdsWithDifferentFieldIds() {
    try {
      IcebergTableMetadataValidator.validateFieldIds(schema1, schema2);
      Assert.fail("Should throw an Illegal Argument Exception");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("Field Ids Mismatch between Source and Destination Iceberg Tables Schema"));
    } catch (Exception e) {
      Assert.fail("Should throw an Illegal Argument Exception");
    }
  }

  @Test
  public void testValidateSamePartitionSpec() {
    try {
      IcebergTableMetadataValidator.validatePartitionSpec(partitionSpec1, partitionSpec1);
    } catch (Exception e) {
      System.out.println(e.getMessage());
      Assert.fail("Should not throw any exception");
    }
  }

  @Test
  public void testValidatePartitionSpecWithDiffType() {
    PartitionSpec partitionSpec4 = PartitionSpec.builderFor(schema4)
        .identity("field1")
        .build();
    try {
      IcebergTableMetadataValidator.validatePartitionSpec(partitionSpec1, partitionSpec4);
      Assert.fail("Should throw an Illegal Argument Exception");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("Partition Spec Have different types for same partition field between Source and Destination Iceberg Table"));
    } catch (Exception e) {
      Assert.fail("Should throw an Illegal Argument Exception");
    }
  }

  @Test
  public void testValidatePartitionSpecWithDiffName() {
    try {
      IcebergTableMetadataValidator.validatePartitionSpec(partitionSpec1, partitionSpec2);
      Assert.fail("Should throw an Illegal Argument Exception");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("Partition Spec Mismatch between Source and Destination Iceberg Tables Partition Spec"));
    } catch (Exception e) {
      Assert.fail("Should throw an Illegal Argument Exception");
    }
  }

  @Test
  public void testValidatePartitionSpecWithDiffNameAndDiffType() {
    try {
      IcebergTableMetadataValidator.validatePartitionSpec(partitionSpec1, partitionSpec5);
      Assert.fail("Should throw an Illegal Argument Exception");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("Partition Spec Mismatch between Source and Destination Iceberg Tables Partition Spec"));
    } catch (Exception e) {
      Assert.fail("Should throw an Illegal Argument Exception");
    }
  }

  @Test
  public void testValidatePartitionSpecWithDiffFieldId() {
    try {
      IcebergTableMetadataValidator.validatePartitionSpec(partitionSpec12, partitionSpec2);
      Assert.fail("Should throw an Illegal Argument Exception");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("Partition Spec Mismatch between Source and Destination Iceberg Tables Partition Spec"));
    } catch (Exception e) {
      Assert.fail("Should throw an Illegal Argument Exception");
    }
  }

  @Test
  public void testValidatePartitionSpecWithDiffFieldIdAndDiffName() {
    try {
      IcebergTableMetadataValidator.validatePartitionSpec(partitionSpec12, partitionSpec5);
      Assert.fail("Should throw an Illegal Argument Exception");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("Partition Spec Mismatch between Source and Destination Iceberg Tables Partition Spec"));
    } catch (Exception e) {
      Assert.fail("Should throw an Illegal Argument Exception");
    }
  }

  @Test
  public void testValidatePartitionSpecWithUnpartitioned() {
    PartitionSpec partitionSpec3 = PartitionSpec.unpartitioned();
    try {
      IcebergTableMetadataValidator.validatePartitionSpec(partitionSpec1, partitionSpec3);
      Assert.fail("Should throw an Illegal Argument Exception");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("Partition Spec Mismatch between Source and Destination Iceberg Tables Partition Spec"));
    } catch (Exception e) {
      Assert.fail("Should throw an Illegal Argument Exception");
    }
  }

  @Test
  public void testPartitionSpecWithDifferentTransform() {
    PartitionSpec partitionSpec = PartitionSpec.builderFor(schema2)
        .truncate("field2", 4)
        .build();
    try {
      IcebergTableMetadataValidator.validatePartitionSpec(partitionSpec1, partitionSpec);
      Assert.fail("Should throw an Illegal Argument Exception");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().startsWith("Partition Spec Mismatch between Source and Destination Iceberg Tables Partition Spec"));
    } catch (Exception e) {
      Assert.fail("Should throw an Illegal Argument Exception");
    }
  }

  @Test
  public void testRandom() {

//    System.out.println(partitionSpec1);
//    System.out.println(partitionSpec2);
//    System.out.println(partitionSpec3);
//    System.out.println(partitionSpec4);
//    System.out.println(partitionSpec5);
//
//    System.out.println(partitionSpec1.partitionType());
//    System.out.println(partitionSpec2.partitionType());
//    System.out.println(partitionSpec3.partitionType());
//    System.out.println(partitionSpec4.partitionType());
//    System.out.println(partitionSpec5.partitionType());
//
//    System.out.println(Arrays.toString(partitionSpec1.javaClasses()));
//    System.out.println(Arrays.toString(partitionSpec2.javaClasses()));
//    System.out.println(Arrays.toString(partitionSpec3.javaClasses()));
//    System.out.println(Arrays.toString(partitionSpec4.javaClasses()));
//    System.out.println(Arrays.toString(partitionSpec5.javaClasses()));
//
//    System.out.println(Arrays.equals(partitionSpec1.javaClasses(), partitionSpec2.javaClasses()));
//    System.out.println(Arrays.equals(partitionSpec1.javaClasses(), partitionSpec3.javaClasses()));
//    System.out.println(Arrays.equals(partitionSpec1.javaClasses(), partitionSpec4.javaClasses()));
//    System.out.println(Arrays.equals(partitionSpec2.javaClasses(), partitionSpec3.javaClasses()));
//    System.out.println(Arrays.equals(partitionSpec2.javaClasses(), partitionSpec4.javaClasses()));
//    System.out.println(Arrays.equals(partitionSpec3.javaClasses(), partitionSpec4.javaClasses()));
//
//    System.out.println(Arrays.toString(partitionSpec5.javaClasses()));

//    System.out.println(partitionSpec3.partitionType().equals(partitionSpec3.partitionType()));
//    System.out.println(partitionSpec3.partitionType());
//    System.out.println(partitionSpec4.partitionType());
//
//    System.out.println(partitionSpec1.partitionType().equals(partitionSpec2.partitionType()));
//    System.out.println(partitionSpec1.partitionType());
//    System.out.println(partitionSpec2.partitionType());
//
//    System.out.println();
//
//    System.out.println(partitionSpec1.equals(partitionSpec2));
//    System.out.println(partitionSpec1.equals(partitionSpec3));
//    System.out.println(partitionSpec3.equals(partitionSpec4));
//    System.out.println(partitionSpec1.equals(partitionSpec4));
//    System.out.println(partitionSpec2.equals(partitionSpec4));
//
//    IcebergTableMetadataValidator.validatePartitionSpec(partitionSpec3, partitionSpec4);
  }

}

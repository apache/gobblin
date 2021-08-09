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

package org.apache.gobblin.util.orc;

import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.orc.TypeDescription;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.gobblin.util.orc.AvroOrcSchemaConverter.*;


public class AvroOrcSchemaConverterTest {
  @Test
  public void testUnionORCSchemaTranslation() throws Exception {
    Schema avroUnion = SchemaBuilder.record("test")
        .fields()
        .name("test_union")
        .type(SchemaBuilder.builder().unionOf().stringType().and().intType().and().nullType().endUnion())
        .noDefault()
        .endRecord();

    TypeDescription unionSchema = TypeDescription.createUnion()
        .addUnionChild(TypeDescription.createString())
        .addUnionChild(TypeDescription.createInt());
    TypeDescription recordSchemaWithUnion = TypeDescription.createStruct().addField("test_union", unionSchema);

    // Verify the schema conversion for Union works
    Assert.assertEquals(AvroOrcSchemaConverter.getOrcSchema(avroUnion), recordSchemaWithUnion);

    //Create a nullable union field
    Schema nullableAvroUnion = SchemaBuilder.record("test")
        .fields()
        .name("test_union")
        .type(SchemaBuilder.builder().unionOf().stringType().and().nullType().endUnion())
        .noDefault()
        .endRecord();
    //Assert that Orc schema has flattened the nullable union to the member's type
    Assert.assertEquals(AvroOrcSchemaConverter.getOrcSchema(nullableAvroUnion),
        TypeDescription.createStruct().addField("test_union", TypeDescription.createString()));

    //Create a non nullable union type
    Schema nonNullableAvroUnion = SchemaBuilder.record("test")
        .fields()
        .name("test_union")
        .type(SchemaBuilder.builder().unionOf().stringType().endUnion())
        .noDefault()
        .endRecord();
    //Ensure that the union type is preserved
    Assert.assertEquals(AvroOrcSchemaConverter.getOrcSchema(nonNullableAvroUnion), TypeDescription.createStruct()
        .addField("test_union", TypeDescription.createUnion().addUnionChild(TypeDescription.createString())));
  }

  @Test
  public void testTrivialAvroSchemaTranslation() throws Exception {
    Schema decimalSchema = SchemaBuilder.builder().bytesType();
    decimalSchema.addProp(LogicalType.LOGICAL_TYPE_PROP, "decimal");
    decimalSchema.addProp("scale", 2);
    decimalSchema.addProp("precision", 10);

    // Trivial cases
    Schema avroSchema = SchemaBuilder.record("test")
        .fields()
        .name("string_type")
        .type(SchemaBuilder.builder().stringType())
        .noDefault()
        .name("int_type")
        .type(SchemaBuilder.builder().intType())
        .noDefault()
        .name("decimal_type")
        .type(decimalSchema)
        .noDefault()
        .endRecord();

    TypeDescription orcSchema = TypeDescription.createStruct()
        .addField("string_type", TypeDescription.createString())
        .addField("int_type", TypeDescription.createInt())
        .addField("decimal_type", TypeDescription.createDecimal().withPrecision(10).withScale(2));

    // Top-level record name will not be replicated in conversion result.
    Assert.assertEquals(avroSchema.getFields(), getAvroSchema(orcSchema).getFields());

    Assert.assertEquals(AvroOrcSchemaConverter.getOrcSchema(avroSchema), orcSchema);
  }

  @Test
  public void testUnionAvroSchemaTranslation() throws Exception {
    Schema avroSchema = SchemaBuilder.record("test")
        .fields()
        .name("union_nested")
        .type(SchemaBuilder.builder().unionOf().stringType().and().intType().endUnion())
        .noDefault()
        .endRecord();
    TypeDescription orcSchema = TypeDescription.createStruct()
        .addField("union_nested", TypeDescription.createUnion()
            .addUnionChild(TypeDescription.createString())
            .addUnionChild(TypeDescription.createInt()));

    Assert.assertEquals(avroSchema.getFields(), getAvroSchema(orcSchema).getFields());
  }

  @Test
  public void testSchemaSanitization() throws Exception {

    // Two field along with null
    Schema avroSchema = SchemaBuilder.builder().unionOf().nullType().and().stringType().and().intType().endUnion();
    Schema expectedSchema = SchemaBuilder.builder().unionOf().stringType().and().intType().endUnion();
    Assert.assertEquals(sanitizeNullableSchema(avroSchema), expectedSchema);

    // Only one field except null
    Schema avroSchema_1 = SchemaBuilder.builder()
        .unionOf()
        .nullType()
        .and()
        .record("test")
        .fields()
        .name("aaa")
        .type(SchemaBuilder.builder().intType())
        .noDefault()
        .endRecord()
        .endUnion();
    expectedSchema = SchemaBuilder.builder()
        .record("test")
        .fields()
        .name("aaa")
        .type(SchemaBuilder.builder().intType())
        .noDefault()
        .endRecord();
    Assert.assertEquals(sanitizeNullableSchema(avroSchema_1), expectedSchema);
  }

  public static Schema getAvroSchema(TypeDescription schema) {
    final TypeDescription.Category type = schema.getCategory();
    switch (type) {
      case BYTE:
      case SHORT:
      case DATE:
      case TIMESTAMP:
      case VARCHAR:
      case CHAR:
        throw new UnsupportedOperationException("Types like BYTE and SHORT (and many more) are not supported in Avro");
      case DECIMAL:
        Schema bytesType = SchemaBuilder.builder().bytesType();
        bytesType.addProp(LogicalType.LOGICAL_TYPE_PROP, "decimal");
        bytesType.addProp("scale", schema.getScale());
        bytesType.addProp("precision", schema.getPrecision());

        return bytesType;
      case BOOLEAN:
        return SchemaBuilder.builder().booleanType();
      case INT:
        return SchemaBuilder.builder().intType();
      case LONG:
        return SchemaBuilder.builder().longType();
      case STRUCT:
        // TODO: Cases that current implementation cannot support:
        // union<struct1, struct2, ..., structN>
        // All these structs will be assigned with the same name, while calling "endUnion" an exception will be thrown.
        // We would workaround this by assigning randomly-picked name while that will cause difficulties in name-related
        // resolution after translation, like `resolveUnion` method which is relying on name.
        SchemaBuilder.FieldAssembler assembler = SchemaBuilder.record("nested").fields();
        List<String> childFieldNames = schema.getFieldNames();
        List<TypeDescription> childrenSchemas = schema.getChildren();
        for (int i = 0; i < childrenSchemas.size(); i++) {
          String fieldName = childFieldNames.get(i);
          assembler = assembler.name(fieldName).type(getAvroSchema(childrenSchemas.get(i))).noDefault();
        }
        return (Schema) assembler.endRecord();
      case STRING:
        return SchemaBuilder.builder().stringType();
      case BINARY:
        return SchemaBuilder.builder().bytesType();
      case DOUBLE:
        return SchemaBuilder.builder().doubleType();
      case FLOAT:
        return SchemaBuilder.builder().floatType();
      case LIST:
        return SchemaBuilder.builder().array().items(getAvroSchema(schema.getChildren().get(0)));
      case MAP:
        Preconditions.checkArgument(schema.getChildren().get(0).getCategory().equals(TypeDescription.Category.STRING));
        Preconditions.checkArgument(schema.getChildren().size() == 2);
        return SchemaBuilder.builder().map().values(getAvroSchema(schema.getChildren().get(1)));
      case UNION:
        SchemaBuilder.BaseTypeBuilder builder = SchemaBuilder.builder().unionOf();
        List<TypeDescription> unionChildrenSchemas = schema.getChildren();
        for (int i = 0; i < unionChildrenSchemas.size(); i++) {
          if (i < unionChildrenSchemas.size() - 1) {
            builder = ((SchemaBuilder.UnionAccumulator<Schema>) builder.type(
                getAvroSchema(unionChildrenSchemas.get(i)))).and();
          } else {
            return ((SchemaBuilder.UnionAccumulator<Schema>) builder.type(
                getAvroSchema(unionChildrenSchemas.get(i)))).endUnion();
          }
        }
      default:
        throw new IllegalStateException("Unrecognized ORC type:" + schema.getCategory());
    }
  }
}
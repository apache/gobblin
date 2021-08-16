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

package org.apache.gobblin.writer;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.util.orc.AvroOrcSchemaConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.orc.OrcFile;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcUnion;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.io.Files;

import lombok.extern.slf4j.Slf4j;

import static org.apache.orc.mapred.OrcMapredRecordReader.nextValue;


@Slf4j
public class GenericRecordToOrcValueWriterTest {
  @Test
  public void testUnionRecordConversionWriter()
      throws Exception {
    Schema schema =
        new Schema.Parser().parse(this.getClass().getClassLoader().getResourceAsStream("union_test/schema.avsc"));

    TypeDescription orcSchema = AvroOrcSchemaConverter.getOrcSchema(schema);
    GenericRecordToOrcValueWriter valueWriter = new GenericRecordToOrcValueWriter(orcSchema, schema);
    VectorizedRowBatch rowBatch = orcSchema.createRowBatch();

    List<GenericRecord> recordList = GobblinOrcWriterTest
        .deserializeAvroRecords(this.getClass(), schema, "union_test/data.json");
    for (GenericRecord record : recordList) {
      valueWriter.write(record, rowBatch);
    }

    // Flush RowBatch into disk.
    File tempFile = new File(Files.createTempDir(), "orc");
    tempFile.deleteOnExit();
    Path filePath = new Path(tempFile.getAbsolutePath());

    OrcFile.WriterOptions options = OrcFile.writerOptions(new Properties(), new Configuration());
    options.setSchema(orcSchema);
    Writer orcFileWriter = OrcFile.createWriter(filePath, options);
    orcFileWriter.addRowBatch(rowBatch);
    orcFileWriter.close();

    // Load it back and compare.
    FileSystem fs = FileSystem.get(new Configuration());
    List<Writable> orcRecords = deserializeOrcRecords(filePath, fs);

    Assert.assertEquals(orcRecords.size(), 5);

    // Knowing all of them are OrcStruct<OrcUnion>, save the effort to recursively convert GenericRecord to OrcStruct
    // for comprehensive comparison which is non-trivial,
    // although it is also theoretically possible and optimal way for doing this unit test.
    List<OrcUnion> unionList = orcRecords.stream().map(this::getUnionFieldFromStruct).collect(Collectors.toList());

    // Constructing all OrcUnion and verify all of them appears in unionList.
    TypeDescription unionSchema = orcSchema.getChildren().get(0);
    OrcUnion union_0 = new OrcUnion(unionSchema);
    union_0.set((byte) 0, new Text("urn:li:member:3"));
    Assert.assertTrue(unionList.contains(union_0));

    OrcUnion union_1 = new OrcUnion(unionSchema);
    union_1.set((byte) 0, new Text("urn:li:member:4"));
    Assert.assertTrue(unionList.contains(union_1));

    OrcUnion union_2 = new OrcUnion(unionSchema);
    union_2.set((byte) 1, new IntWritable(2));
    Assert.assertTrue(unionList.contains(union_2));

    OrcUnion union_3 = new OrcUnion(unionSchema);
    union_3.set((byte) 1, new IntWritable(1));
    Assert.assertTrue(unionList.contains(union_3));

    OrcUnion union_4 = new OrcUnion(unionSchema);
    union_4.set((byte) 1, new IntWritable(3));
    Assert.assertTrue(unionList.contains(union_4));
  }

  @Test
  public void testDecimalRecordConversionWriter()
      throws Exception {
    Schema schema =
        new Schema.Parser().parse(this.getClass().getClassLoader().getResourceAsStream("decimal_test/schema.avsc"));

    TypeDescription orcSchema = AvroOrcSchemaConverter.getOrcSchema(schema);
    GenericRecordToOrcValueWriter valueWriter = new GenericRecordToOrcValueWriter(orcSchema, schema);
    VectorizedRowBatch rowBatch = orcSchema.createRowBatch();

    List<GenericRecord> recordList = GobblinOrcWriterTest
        .deserializeAvroRecords(this.getClass(), schema, "decimal_test/data.json");
    for (GenericRecord record : recordList) {
      valueWriter.write(record, rowBatch);
    }

    // Flush RowBatch into disk.
    File tempFile = new File(Files.createTempDir(), "orc");
    tempFile.deleteOnExit();
    Path filePath = new Path(tempFile.getAbsolutePath());

    OrcFile.WriterOptions options = OrcFile.writerOptions(new Properties(), new Configuration());
    options.setSchema(orcSchema);
    Writer orcFileWriter = OrcFile.createWriter(filePath, options);
    orcFileWriter.addRowBatch(rowBatch);
    orcFileWriter.close();

    // Load it back and compare.
    FileSystem fs = FileSystem.get(new Configuration());
    List<Writable> orcRecords = deserializeOrcRecords(filePath, fs);

    Assert.assertEquals(orcRecords.size(), 2);
    Assert.assertEquals(orcRecords.get(0).toString(), "{3.4}");
    Assert.assertEquals(orcRecords.get(1).toString(), "{5.97}");
  }

  @Test
  public void testListResize()
      throws Exception {
    Schema schema =
        new Schema.Parser().parse(this.getClass().getClassLoader().getResourceAsStream("list_map_test/schema.avsc"));

    TypeDescription orcSchema = AvroOrcSchemaConverter.getOrcSchema(schema);
    GenericRecordToOrcValueWriter valueWriter = new GenericRecordToOrcValueWriter(orcSchema, schema);
    // Make the batch size very small so that the enlarge behavior would easily be triggered.
    // But this has to more than the number of records that we deserialized form data.json, as here we don't reset batch.
    VectorizedRowBatch rowBatch = orcSchema.createRowBatch(10);

    List<GenericRecord> recordList = GobblinOrcWriterTest
        .deserializeAvroRecords(this.getClass(), schema, "list_map_test/data.json");
    Assert.assertEquals(recordList.size(), 6);
    for (GenericRecord record : recordList) {
      valueWriter.write(record, rowBatch);
    }
    // Examining resize count, which should happen only once for map and list, so totally 2.
    Assert.assertEquals(valueWriter.resizeCount, 2);
  }

  /**
   * Accessing "fields" using reflection to work-around access modifiers.
   */
  private OrcUnion getUnionFieldFromStruct(Writable struct) {
    try {
      OrcStruct orcStruct = (OrcStruct) struct;
      Field objectArr = OrcStruct.class.getDeclaredField("fields");
      objectArr.setAccessible(true);
      return (OrcUnion) ((Object[]) objectArr.get(orcStruct))[0];
    } catch (Exception e) {
      throw new RuntimeException("Cannot access with reflection", e);
    }
  }

  public static final List<Writable> deserializeOrcRecords(Path orcFilePath, FileSystem fs)
      throws IOException {
    org.apache.orc.Reader fileReader = OrcFile.createReader(orcFilePath, new OrcFile.ReaderOptions(new Configuration()));
    RecordReader recordReader = fileReader.rows();
    TypeDescription schema = fileReader.getSchema();
    VectorizedRowBatch batch = schema.createRowBatch();
    recordReader.nextBatch(batch);
    int rowInBatch = 0;

    // result container
    List<Writable> orcRecords = new ArrayList<>();

    long rowCount = fileReader.getNumberOfRows();
    while (rowCount > 0) {
      // Deserialize records using Mapreduce-like API
      if (schema.getCategory() == TypeDescription.Category.STRUCT) {
        OrcStruct result = (OrcStruct) OrcStruct.createValue(fileReader.getSchema());
        List<TypeDescription> children = schema.getChildren();
        int numberOfChildren = children.size();
        for (int i = 0; i < numberOfChildren; ++i) {
          result.setFieldValue(i, nextValue(batch.cols[i], rowInBatch, children.get(i), result.getFieldValue(i)));
        }
        orcRecords.add(result);
      } else {
        throw new UnsupportedOperationException("The serialized records have to be a struct in the outer-most layer.");
      }
      rowCount -= 1;
      rowInBatch += 1;
    }
    return orcRecords;
  }
}
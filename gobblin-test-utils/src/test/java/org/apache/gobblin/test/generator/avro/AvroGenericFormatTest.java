package org.apache.gobblin.test.generator.avro;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.testng.annotations.Test;

import org.apache.gobblin.test.AvroGenericFormat;


public class AvroGenericFormatTest {

  @Test
  public void testSingleRecord()
      throws IOException {
    AvroGenericFormat format = new AvroGenericFormat();
    for (int i=0; i < 20; ++i) {
      Schema schema = format.generateRandomSchema(Collections.EMPTY_LIST);
      File file=new File("data"+i+".avro");
      DatumWriter<GenericRecord> writer=new GenericDatumWriter<GenericRecord>(schema);
      DataFileWriter<GenericRecord> dataFileWriter=new DataFileWriter<GenericRecord>(writer);
      dataFileWriter.create(schema, file);
      for (int j=0; j < 2000; ++j) {
        GenericRecord record = format.generateRandomRecord();
        System.out.println(record);
        dataFileWriter.append(record);
      }
      dataFileWriter.close();
    }
  }
}

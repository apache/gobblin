package org.apache.gobblin.test.generator.avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.testng.annotations.Test;

import org.apache.gobblin.test.generator.FixedSchemaStructGeneratorTestBase;
import org.apache.gobblin.test.generator.FixedStructValueGenerator;
import org.apache.gobblin.test.generator.config.FixedSchemaGeneratorConfig;


public class AvroFixedSchemaStructGeneratorTest extends FixedSchemaStructGeneratorTestBase<AvroFixedSchemaStructGenerator> {


  @Override
  protected FixedStructValueGenerator constructGenerator(FixedSchemaGeneratorConfig config) {
    return new AvroFixedSchemaStructGenerator(config);
  }

  @Override
  protected Writer getFileWriter(FixedStructValueGenerator generator, File file)
      throws IOException {
    final DatumWriter<GenericRecord> writer = new GenericDatumWriter<>();
    final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(writer);
    dataFileWriter.create(((AvroFixedSchemaStructGenerator) generator).getAvroSchema(), file);

    return new Writer<GenericRecord>() {

      @Override
      public void write(GenericRecord record)
          throws IOException {
        dataFileWriter.append(record);
      }

      @Override
      public void close()
          throws IOException {
        dataFileWriter.close();
      }
    };
  }

  @Override
  protected Reader getFileReader(FixedStructValueGenerator generator, File file)
      throws IOException {
    final DatumReader<GenericRecord> reader = new GenericDatumReader<>();
    final DataFileReader<GenericRecord> dataFileReader = new DataFileReader(file, reader);
    return new Reader() {
      @Override
      public Object read()
          throws IOException {
        if (dataFileReader.hasNext()) {
          return dataFileReader.next();
        } else {
          return null;
        }
      }

      @Override
      public void close()
          throws IOException {
        dataFileReader.close();
      }
    };
  }

  @Test
  public void testConstructor()
      throws IOException {
    super.testConstructor(AvroFixedSchemaStructGenerator.class);
  }

  @Test
  public void parseConfigTest()
      throws IOException {
    super.parseConfigTest();
  }


}

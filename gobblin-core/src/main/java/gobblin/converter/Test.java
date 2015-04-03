package gobblin.converter;

import gobblin.util.AvroUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;


public class Test {

  public static void main(String[] args) throws IOException, DataConversionException {
    test1();

  }

  private static void test1() throws IOException, DataConversionException {
    Schema full = new Schema.Parser().parse(new File("/Users/ziliu/Documents/avrotest/pve.avsc"));
    Schema less = new Schema.Parser().parse(new File("/Users/ziliu/Documents/avrotest/pve2.avsc"));
    GenericRecord pve = read(full, "/Users/ziliu/Documents/avrotest/pve.json");
    GenericRecord pve2 = read(less, "/Users/ziliu/Documents/avrotest/pve2.json");
    GenericRecord pve3 = AvroUtils.evolveSchema(pve, less);
    GenericRecord pve4 = AvroUtils.evolveSchema(pve2, full);

    DatumWriter<GenericRecord> dw = new GenericDatumWriter<GenericRecord>(full);
    DataFileWriter writer = new DataFileWriter(dw);
    writer.create(full, new File("test.avro"));
    writer.append(pve4);
    writer.close();
  }

  private static GenericRecord read(Schema s, String path) throws FileNotFoundException, IOException {
    Decoder d = new DecoderFactory().jsonDecoder(s, new FileInputStream(path));
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(s);
    return reader.read(null, d);
  }
}

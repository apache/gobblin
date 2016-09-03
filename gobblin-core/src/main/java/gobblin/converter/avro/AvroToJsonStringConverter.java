package gobblin.converter.avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.io.output.ByteArrayOutputStream;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;


/**
 * Converts an Avro record to a json string.
 */
public class AvroToJsonStringConverter extends Converter<Schema, String, GenericRecord, String> {

  private Schema schema;

  private final ThreadLocal<Serializer> serializer = new ThreadLocal<Serializer>() {
    @Override
    protected Serializer initialValue() {
      return new Serializer(AvroToJsonStringConverter.this.schema);
    }
  };

  private static class Serializer {

    private final Encoder encoder;
    private final GenericDatumWriter<GenericRecord> writer;
    private final ByteArrayOutputStream outputStream;

    public Serializer(Schema schema) {
      try {
        this.writer = new GenericDatumWriter<>(schema);
        this.outputStream = new ByteArrayOutputStream();
        this.encoder = EncoderFactory.get().jsonEncoder(schema, this.outputStream);
      } catch (IOException ioe) {
        throw new RuntimeException("Could not initialize avro json encoder.");
      }
    }

    public String serialize(GenericRecord record) throws IOException {
      this.outputStream.reset();
      this.writer.write(record, this.encoder);
      this.encoder.flush();
      return this.outputStream.toString(Charsets.UTF_8);
    }
  }

  @Override
  public String convertSchema(Schema inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    this.schema = inputSchema;
    return this.schema.toString();
  }

  @Override
  public Iterable<String> convertRecord(String outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    try {
      return Lists.newArrayList(this.serializer.get().serialize(inputRecord));
    } catch (IOException ioe) {
      throw new DataConversionException(ioe);
    }
  }
}

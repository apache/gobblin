package org.apache.gobblin.test.generator.avro;

import java.nio.ByteBuffer;

import org.apache.gobblin.test.InMemoryFormat;
import org.apache.gobblin.test.generator.ValueConverter;
import org.apache.gobblin.test.generator.ValueConvertingFunction;
import org.apache.gobblin.test.type.Type;


@ValueConverter(inMemoryType= InMemoryFormat.AVRO_GENERIC, logicalTypes= Type.Bytes)
public class AvroByteArrayConverter implements ValueConvertingFunction<byte[], ByteBuffer> {
  @Override
  public ByteBuffer apply(byte[] bytes) {
    return ByteBuffer.wrap(bytes);
  }
}

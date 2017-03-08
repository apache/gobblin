/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.writer;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;


/**
 * An implementation of {@link DataWriter} that writes json objects directly to HDFS.
 *
 * This class accepts one new configuration parameters:
 * <ul>
 * <li>{@link SIMPLE_WRITER_COMPRESSION_CODEC} accepts a codec name, which implements the
 * {@link org.apache.hadoop.io.compress.CompressionCodec} interface like {@link org.apache.hadoop.io.compress.GzipCodec}
 * </ul>
 */
public class SimpleJsonCompressionDataWriter extends FsDataWriter<JsonElement> {

  private final Optional<Byte> recordDelimiter; // optional byte to place between each record write

  private int recordsWritten;
  private int bytesWritten;
  private Optional<String> codecName;
  static final Charset CHARSET = StandardCharsets.UTF_8;


  private final OutputStream stagingFileOutputStream;

  public static final String SIMPLE_WRITER_COMPRESSION_CODEC = "simple.writer.compression.codec";

  public SimpleJsonCompressionDataWriter(SimpleJsonCompressionDataWriterBuilder builder, State properties) throws IOException {
    super(builder, properties);
    String delim;
    if ((delim = properties.getProp(ConfigurationKeys.SIMPLE_WRITER_DELIMITER, null)) == null || delim.length() == 0) {
      this.recordDelimiter = Optional.absent();
    } else {
      this.recordDelimiter = Optional.of(delim.getBytes(ConfigurationKeys.DEFAULT_CHARSET_ENCODING)[0]);
    }

    this.codecName = Optional.of(properties.getProp(SIMPLE_WRITER_COMPRESSION_CODEC, null));
    this.recordsWritten = 0;
    this.bytesWritten = 0;
    if (codecName.isPresent()) {
      Configuration configuration = new Configuration();
      try {
        CompressionCodecFactory.setCodecClasses(configuration,new LinkedList<Class>(Collections.singletonList(Class.forName(codecName.get()))));
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Unable to find codec class "+codecName.get());
      }
      CompressionCodecFactory codecFactory = new CompressionCodecFactory(
          configuration);
      CompressionCodec codec = codecFactory.getCodecByClassName(this.codecName.get());
      this.stagingFileOutputStream = this.closer.register(codec.createOutputStream(createStagingFileOutputStream()));
    }else {
      this.stagingFileOutputStream = createStagingFileOutputStream();

    }

    setStagingFileGroup();
  }

  /**
   * Write a source record to the staging file
   *
   * @param record data record to write
   * @throws IOException if there is anything wrong writing the record
   */
  @Override
  public void write(JsonElement record) throws IOException {
    Preconditions.checkNotNull(record);

    byte[] toWrite;
    byte[] recordBytes = record.toString().getBytes(CHARSET);

    if (this.recordDelimiter.isPresent()) {
      toWrite = Arrays.copyOf(recordBytes, recordBytes.length + 1);
      toWrite[toWrite.length - 1] = this.recordDelimiter.get();
    }else {
      toWrite = recordBytes;
    }
    this.stagingFileOutputStream.write(toWrite);
    this.bytesWritten += toWrite.length;
    this.recordsWritten++;
  }

  /**
   * Get the number of records written.
   *
   * @return number of records written
   */
  @Override
  public long recordsWritten() {
    return this.recordsWritten;
  }

  /**
   * Get the number of bytes written.
   *
   * @return number of bytes written
   */
  @Override
  public long bytesWritten() throws IOException {
    return this.bytesWritten;
  }

  @Override
  public boolean isSpeculativeAttemptSafe() {
    return this.writerAttemptIdOptional.isPresent() && this.getClass() == SimpleJsonCompressionDataWriter.class;
  }
}

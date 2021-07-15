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

package org.apache.gobblin.source.extractor;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;
import com.google.gson.Gson;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.hadoop.HadoopFsHelper;

/**
 * An implementation of {@link Extractor}.
 *
 * <p>
 *   This extractor reads the assigned input file storing
 *   json documents confirming to a schema. Each line of the file is a json document.
 * </p>
 */
public class SimpleJsonExtractor implements Extractor<String, String> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleJsonExtractor.class);
  private final WorkUnitState workUnitState;
  private final FileSystem fs;
  private final BufferedReader bufferedReader;
  private final Closer closer = Closer.create();
  private static final Gson GSON = new Gson();

  public SimpleJsonExtractor(WorkUnitState workUnitState) throws IOException {
    this.workUnitState = workUnitState;

    HadoopFsHelper fsHelper = new HadoopFsHelper(workUnitState);
    try {
      fsHelper.connect();
    } catch (Exception e) {
      throw new IOException("Exception at SimpleJsonExtractor");
    }
    // Source is responsible to set SOURCE_FILEBASED_FILES_TO_PULL
    this.fs = fsHelper.getFileSystem();
    InputStreamReader isr = new InputStreamReader(this.fs.open(
        new Path(workUnitState.getProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL))), StandardCharsets.UTF_8);

    this.bufferedReader =
        this.closer.register(new BufferedReader(isr));
  }

  @Override
  public String getSchema() throws IOException {
    // Source is responsible to set SOURCE_SCHEMA
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    IOUtils.copyBytes(fs.open(
        new Path(workUnitState.getProp(ConfigurationKeys.SOURCE_SCHEMA))), outputStream, 4096, false);
    String schema = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
    workUnitState.setProp((ConfigurationKeys.CONVERTER_AVRO_SCHEMA_KEY), schema);
    return schema;
  }

  @Override
  public String readRecord(@Deprecated String reuse) throws DataRecordException, IOException {
    return this.bufferedReader.readLine();
  }

  @Override
  public long getExpectedRecordCount() {
    // We don't know how many records are in the file before actually reading them
    return 0;
  }

  @Override
  public long getHighWatermark() {
    // Watermark is not applicable for this type of extractor
    return 0;
  }

  @Override
  public void close() throws IOException {
    try {
      this.closer.close();
    } catch (IOException ioe) {
      LOGGER.error("Failed to close the input stream", ioe);
    }

    try {
      fs.close();
    } catch (IOException ioe) {
      LOGGER.error("Failed to close the file object", ioe);
    }
  }
}

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

import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;

import org.apache.gobblin.codec.StreamCodec;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.AvroUtils;
import org.apache.gobblin.util.ForkOperatorUtils;
import org.apache.gobblin.util.WriterUtils;


/**
 * A abstract {@link DataWriterBuilder} for building {@link DataWriter}s that write to
 * {@link org.apache.hadoop.fs.FileSystem}s.
 *
 * @param <S> schema type
 * @param <S> data record type
 *
 * @author Ziyang Liu
 */
public abstract class FsDataWriterBuilder<S, D> extends PartitionAwareDataWriterBuilder<S, D> {

  public static final String WRITER_INCLUDE_PARTITION_IN_FILE_NAMES =
      ConfigurationKeys.WRITER_PREFIX + ".include.partition.in.file.names";
  public static final String WRITER_REPLACE_PATH_SEPARATORS_IN_PARTITIONS =
      ConfigurationKeys.WRITER_PREFIX + ".replace.path.separators.in.partitions";
  private List<StreamCodec> encoders;

  /**
   * Get the file name to be used by the writer. If a {@link org.apache.gobblin.writer.partitioner.WriterPartioner} is used,
   * the partition will be added as part of the file name.
   */
  public String getFileName(State properties) {

    String extension =
        this.format.equals(WriterOutputFormat.OTHER) ? getExtension(properties) : this.format.getExtension();
    String fileName = WriterUtils.getWriterFileName(properties, this.branches, this.branch, this.writerId, extension);

    if (this.partition.isPresent()) {
      fileName = getPartitionedFileName(properties, fileName);
    }

    List<StreamCodec> encoders = getEncoders();
    if (!encoders.isEmpty()) {
      StringBuilder filenameBuilder = new StringBuilder(fileName);
      for (StreamCodec codec : encoders) {
        filenameBuilder.append('.');
        filenameBuilder.append(codec.getTag());
      }

      fileName = filenameBuilder.toString();
    }

    return fileName;
  }

  private static String getExtension(State properties) {
    return properties.getProp(ConfigurationKeys.WRITER_OUTPUT_FORMAT_KEY, StringUtils.EMPTY);
  }

  protected String getPartitionPath(State properties) {
    if (this.partition.isPresent()) {
      boolean includePartitionerFieldNames = properties.getPropAsBoolean(ForkOperatorUtils
          .getPropertyNameForBranch(WRITER_INCLUDE_PARTITION_IN_FILE_NAMES, this.branches, this.branch), false);
      boolean removePathSeparators = properties.getPropAsBoolean(ForkOperatorUtils
          .getPropertyNameForBranch(WRITER_REPLACE_PATH_SEPARATORS_IN_PARTITIONS, this.branches, this.branch), false);

      return AvroUtils.serializeAsPath(this.partition.get(), includePartitionerFieldNames, removePathSeparators).toString();
    } else {
      return null;
    }
  }

  protected String getPartitionedFileName(State properties, String originalFileName) {
    return new Path(
        getPartitionPath(properties),
        originalFileName).toString();
  }

  @Override
  public boolean validatePartitionSchema(Schema partitionSchema) {
    return true;
  }

  /**
   * Get list of encoders configured for the writer.
   */
  public synchronized List<StreamCodec> getEncoders() {
    if (encoders == null) {
      encoders = buildEncoders();
    }

    return encoders;
  }

  /**
   * Build and cache encoders for the writer based on configured options as encoder
   * construction can potentially be expensive.
   */
  protected List<StreamCodec> buildEncoders() {
    // Should be overridden by subclasses if their associated writers are
    // encoder aware
    return Collections.emptyList();
  }
 }

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

package org.apache.gobblin.iceberg.publisher;

import com.google.common.io.Closer;
import org.apache.gobblin.iceberg.GobblinMCEProducer;
import org.apache.gobblin.metadata.OperationType;
import org.apache.gobblin.metadata.SchemaSource;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.publisher.DataPublisher;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.writer.PartitionedDataWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.orc.OrcMetrics;


/**
 *  A {@link DataPublisher} that compute and emit GobblinMetadataChangeEvent to kafka and rely on metadata ingestion pipeline
 *  to register metadata.
 *
 * <p>
 *   This publisher is not responsible for publishing data, and it relies on another publisher
 *   to document the published paths in property {@link NEW_FILES_LIST}.
 *   This publisher will use {@link GobblinMCEProducer} to emit GMCE events.
 * </p>
 */

@Slf4j
public class GobblinMCEPublisher extends DataPublisher {
  public static final String OFFSET_RANGE_KEY = "offset.range";
  public static final String MAP_DELIMITER_KEY = ":";
  public static final String NEW_FILES_LIST = "new.files.list";
  public static final String AVRO_SCHEMA_WITH_ICEBERG_ID = "avro.schema.with.iceberg.id";
  private final GobblinMCEProducer producer;

  private final Closer closer = Closer.create();
  private final Configuration conf;
  private static final PathFilter HIDDEN_FILES_FILTER = p -> {
    String name = p.getName();
    return !name.startsWith("_") && !name.startsWith(".");
  };

  public GobblinMCEPublisher(State state) throws IOException {

    this(state, GobblinMCEProducer.getGobblinMCEProducer(state));
  }

  public GobblinMCEPublisher(State state, GobblinMCEProducer producer) {
    super(state);
    this.producer = this.closer.register(producer);
    conf = HadoopUtils.getConfFromState(state);
  }

  public void publishData(Collection<? extends WorkUnitState> states) throws IOException {
    // First aggregate the new files by partition
    for (State state : states) {
      Map<Path, Metrics> newFiles = computeFileMetrics(state);
      Map<String, String> offsetRange = getPartitionOffsetRange(OFFSET_RANGE_KEY);
      this.producer.sendGMCE(newFiles, null, null, offsetRange, OperationType.add_files, SchemaSource.SCHEMAREGISTRY);
    }
  }

  private Map<String, String> getPartitionOffsetRange(String offsetKey) {
    return state.getPropAsList(offsetKey)
        .stream()
        .collect(Collectors.toMap(s -> s.split(MAP_DELIMITER_KEY)[0], s -> s.split(MAP_DELIMITER_KEY)[1]));
  }

  /**
   * For each publish  path, get all the data files under path
   * and calculate the hive spec for each datafile and submit the task to register that datafile
   * @throws IOException
   */
  private Map<Path, Metrics> computeFileMetrics(State state) throws IOException {
    Map<Path, Metrics> newFiles = new HashMap<>();
    NameMapping mapping = getNameMapping();

    for (final String pathString : state.getPropAsList(NEW_FILES_LIST, "")) {
      Path path = new Path(pathString);
      FileSystem fs = path.getFileSystem(conf);
      LinkedList<FileStatus> fileStatuses = new LinkedList<>();
      fileStatuses.add(fs.getFileStatus(path));
      // Only register files
      while (!fileStatuses.isEmpty()) {
        FileStatus fileStatus = fileStatuses.pollFirst();
        if (fileStatus.isDirectory()) {
          fileStatuses.addAll(Arrays.asList(fs.listStatus(fileStatus.getPath(), HIDDEN_FILES_FILTER)));
        } else {
          Path filePath = fileStatus.getPath();
          Metrics metrics = getMetrics(state, filePath, conf, mapping);
          newFiles.put(filePath, metrics);
        }
      }
    }
    return newFiles;
  }

  protected NameMapping getNameMapping() {
    String writerSchema = state.getProp(PartitionedDataWriter.WRITER_LATEST_SCHEMA);
    if (writerSchema == null) {
      return null;
    }
    org.apache.iceberg.shaded.org.apache.avro.Schema avroSchema =
        new org.apache.iceberg.shaded.org.apache.avro.Schema.Parser().parse(writerSchema);
    Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
    //This convert is to make sure the schema has the iceberg id setup
    state.setProp(AVRO_SCHEMA_WITH_ICEBERG_ID, AvroSchemaUtil.convert(icebergSchema.asStruct()).toString());
    return MappingUtil.create(icebergSchema);
  }

  public static Metrics getMetrics(State state, Path path, Configuration conf, NameMapping mapping) {
    switch (GobblinMCEProducer.getIcebergFormat(state)) {
      case ORC: {
        return OrcMetrics.fromInputFile(HadoopInputFile.fromPath(path, conf), MetricsConfig.getDefault(), mapping);
      }
      case AVRO: {
        try {
          return new Metrics(100000000L, null, null, null);
        } catch (Exception e) {
          throw new RuntimeException("Cannot get file information for file " + path.toString(), e);
        }
      }
      default: {
        throw new IllegalArgumentException("Unsupported data format for file " + path);
      }
    }
  }

  public void publishMetadata(Collection<? extends WorkUnitState> states) {
  }

  @Deprecated
  @Override
  public void initialize() {
  }

  @Override
  public void close() throws IOException {
    this.closer.close();
  }
}

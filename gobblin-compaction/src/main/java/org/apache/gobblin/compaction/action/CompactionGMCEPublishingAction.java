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

package org.apache.gobblin.compaction.action;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.compaction.mapreduce.CompactionJobConfigurator;
import org.apache.gobblin.compaction.parser.CompactionPathParser;
import org.apache.gobblin.compaction.verify.InputRecordCountHelper;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.dataset.FileSystemDataset;
import org.apache.gobblin.iceberg.GobblinMCEProducer;
import org.apache.gobblin.iceberg.Utils.IcebergUtils;
import org.apache.gobblin.iceberg.publisher.GobblinMCEPublisher;
import org.apache.gobblin.metadata.OperationType;
import org.apache.gobblin.metadata.SchemaSource;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.orc.ORCSchemaUtil;
import org.apache.iceberg.shaded.org.apache.orc.TypeDescription;
import org.apache.orc.OrcConf;


/**
 * Class responsible for emitting GMCE after compaction is complete
 */
@Slf4j
public class CompactionGMCEPublishingAction implements CompactionCompleteAction<FileSystemDataset> {

  public static final String ICEBERG_ID_ATTRIBUTE = "iceberg.id";
  public static final String ICEBERG_REQUIRED_ATTRIBUTE = "iceberg.required";
  public final static String GMCE_EMITTED_KEY = "GMCE.emitted";
  private final State state;
  private final CompactionJobConfigurator configurator;
  private final Configuration conf;
  private InputRecordCountHelper helper;
  private EventSubmitter eventSubmitter;

  public CompactionGMCEPublishingAction(State state, CompactionJobConfigurator configurator, InputRecordCountHelper helper) {
    if (!(state instanceof WorkUnitState)) {
      throw new UnsupportedOperationException(this.getClass().getName() + " only supports workunit state");
    }
    this.state = state;
    this.configurator = configurator;
    this.conf = HadoopUtils.getConfFromState(state);
    this.helper = helper;
  }

  public CompactionGMCEPublishingAction(State state, CompactionJobConfigurator configurator) {
    this(state, configurator, new InputRecordCountHelper(state));
  }

  public void onCompactionJobComplete(FileSystemDataset dataset) throws IOException {
    if (dataset.isVirtual()) {
      return;
    }
    CompactionPathParser.CompactionParserResult result = new CompactionPathParser(state).parse(dataset);
    String datasetDir = Joiner.on("/").join(result.getDstBaseDir(), result.getDatasetName());
    state.setProp(ConfigurationKeys.DATA_PUBLISHER_DATASET_DIR, datasetDir);
    try (GobblinMCEProducer producer = GobblinMCEProducer.getGobblinMCEProducer(state)) {
      producer.sendGMCE(getNewFileMetrics(result), null, Lists.newArrayList(this.configurator.getOldFiles()), null,
          OperationType.rewrite_files, SchemaSource.NONE);
    }
    State compactionState = helper.loadState(new Path(result.getDstAbsoluteDir()));
    //Set the prop to be true to indicate that gmce has been emitted
    compactionState.setProp(GMCE_EMITTED_KEY, true);
    helper.saveState(new Path(result.getDstAbsoluteDir()), compactionState);
    //clear old files to release memory
    this.configurator.getOldFiles().clear();
  }

  private Map<Path, Metrics> getNewFileMetrics(CompactionPathParser.CompactionParserResult result) {
    NameMapping mapping = null;
    try {
      if (IcebergUtils.getIcebergFormat(state) == FileFormat.ORC) {
        String s =
            this.configurator.getConfiguredJob().getConfiguration().get(OrcConf.MAPRED_OUTPUT_SCHEMA.getAttribute());
        TypeDescription orcSchema = TypeDescription.fromString(s);
        for (int i = 0; i <= orcSchema.getMaximumId(); i++) {
          orcSchema.findSubtype(i).setAttribute(ICEBERG_ID_ATTRIBUTE, Integer.toString(i));
          orcSchema.findSubtype(i).setAttribute(ICEBERG_REQUIRED_ATTRIBUTE, Boolean.toString(true));
        }
        Schema icebergSchema = ORCSchemaUtil.convert(orcSchema);
        state.setProp(GobblinMCEPublisher.AVRO_SCHEMA_WITH_ICEBERG_ID,
            AvroSchemaUtil.convert(icebergSchema.asStruct()).toString());
        mapping = MappingUtil.create(icebergSchema);
      }
    } catch (Exception e) {
      log.warn(
          "Table {} contains complex union type which is not compatible with iceberg, will not calculate the metrics for it",
          result.getDatasetName());
    }
    Map<Path, Metrics> newFiles = new HashMap<>();
    for (Path filePath : this.configurator.getDstNewFiles()) {
      newFiles.put(filePath, GobblinMCEPublisher.getMetrics(state, filePath, conf, mapping));
    }
    return newFiles;
  }

  public void addEventSubmitter(EventSubmitter submitter) {
    this.eventSubmitter = submitter;
  }
}

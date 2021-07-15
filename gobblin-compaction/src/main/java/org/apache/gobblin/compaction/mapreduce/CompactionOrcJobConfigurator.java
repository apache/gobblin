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

package org.apache.gobblin.compaction.mapreduce;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcKey;
import org.apache.orc.mapred.OrcValue;

import org.apache.gobblin.compaction.mapreduce.orc.OrcKeyCompactorOutputFormat;
import org.apache.gobblin.compaction.mapreduce.orc.OrcKeyComparator;
import org.apache.gobblin.compaction.mapreduce.orc.OrcKeyDedupReducer;
import org.apache.gobblin.compaction.mapreduce.orc.OrcUtils;
import org.apache.gobblin.compaction.mapreduce.orc.OrcValueCombineFileInputFormat;
import org.apache.gobblin.compaction.mapreduce.orc.OrcValueMapper;
import org.apache.gobblin.configuration.State;

import static org.apache.gobblin.compaction.mapreduce.CompactorOutputCommitter.COMPACTION_OUTPUT_EXTENSION;
import static org.apache.gobblin.compaction.mapreduce.orc.OrcUtils.eligibleForUpConvert;
import static org.apache.gobblin.writer.GobblinOrcWriter.DEFAULT_ORC_WRITER_BATCH_SIZE;
import static org.apache.gobblin.writer.GobblinOrcWriter.ORC_WRITER_BATCH_SIZE;

public class CompactionOrcJobConfigurator extends CompactionJobConfigurator {
  /**
   * The key schema for the shuffle output.
   */
  public static final String ORC_MAPPER_SHUFFLE_KEY_SCHEMA = "orcMapperShuffleSchema";
  private String orcMapperShuffleSchemaString;

  public static class Factory implements CompactionJobConfigurator.ConfiguratorFactory {
    @Override
    public CompactionJobConfigurator createConfigurator(State state) throws IOException {
      return new CompactionOrcJobConfigurator(state);
    }
  }

  public CompactionOrcJobConfigurator(State state) throws IOException {
    super(state);
    this.orcMapperShuffleSchemaString = state.getProp(ORC_MAPPER_SHUFFLE_KEY_SCHEMA, StringUtils.EMPTY);
  }

  @Override
  public String getFileExtension() {
    return this.state.getProp(COMPACTION_OUTPUT_EXTENSION, EXTENSION.ORC.getExtensionString());
  }

  protected void configureSchema(Job job) throws IOException {
    TypeDescription schema = OrcUtils.getNewestSchemaFromSource(job, this.fs);

    job.getConfiguration().set(OrcConf.MAPRED_INPUT_SCHEMA.getAttribute(), schema.toString());

    // Determine the shuffle-schema: Only take the user-specified shuffle-schema if it is upconvertable
    // Check the eligibleForUpConvert method for the definition of eligibility.
    if (!orcMapperShuffleSchemaString.isEmpty()
        && eligibleForUpConvert(schema, TypeDescription.fromString(orcMapperShuffleSchemaString))) {
      job.getConfiguration().set(OrcConf.MAPRED_SHUFFLE_KEY_SCHEMA.getAttribute(), orcMapperShuffleSchemaString);
    } else {
      job.getConfiguration().set(OrcConf.MAPRED_SHUFFLE_KEY_SCHEMA.getAttribute(), schema.toString());
    }

    job.getConfiguration().set(OrcConf.MAPRED_SHUFFLE_VALUE_SCHEMA.getAttribute(), schema.toString());
    job.getConfiguration().set(OrcConf.MAPRED_OUTPUT_SCHEMA.getAttribute(), schema.toString());
  }

  private int getWriterRowBatchSize() {
    return this.state.getPropAsInt(ORC_WRITER_BATCH_SIZE, DEFAULT_ORC_WRITER_BATCH_SIZE);
  }

  protected void setOrcWriterBatchSize(Job job) {
    job.getConfiguration().setInt(ORC_WRITER_BATCH_SIZE, getWriterRowBatchSize());
  }

  protected void configureMapper(Job job) {
    job.setInputFormatClass(OrcValueCombineFileInputFormat.class);
    job.setMapperClass(OrcValueMapper.class);
    job.setMapOutputKeyClass(OrcKey.class);
    job.setMapOutputValueClass(OrcValue.class);
    job.setGroupingComparatorClass(OrcKeyComparator.class);
    job.setSortComparatorClass(OrcKeyComparator.class);
  }

  protected void configureReducer(Job job) throws IOException {
    job.setReducerClass(OrcKeyDedupReducer.class);
    job.setOutputFormatClass(OrcKeyCompactorOutputFormat.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(OrcValue.class);
    setNumberOfReducers(job);
    setOrcWriterBatchSize(job);
  }
}

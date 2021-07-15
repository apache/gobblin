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

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Enums;
import com.google.common.base.Optional;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.compaction.mapreduce.avro.AvroKeyCompactorOutputFormat;
import org.apache.gobblin.compaction.mapreduce.avro.AvroKeyDedupReducer;
import org.apache.gobblin.compaction.mapreduce.avro.AvroKeyMapper;
import org.apache.gobblin.compaction.mapreduce.avro.AvroKeyRecursiveCombineFileInputFormat;
import org.apache.gobblin.compaction.mapreduce.avro.MRCompactorAvroKeyDedupJobRunner;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.converter.filter.AvroSchemaFieldRemover;
import org.apache.gobblin.util.AvroUtils;


/**
 * A configurator that focused on creating avro compaction map-reduce job
 */
@Slf4j
public class CompactionAvroJobConfigurator extends CompactionJobConfigurator {

  private Optional<String> keyFieldBlacklist;

  public static class Factory implements CompactionJobConfigurator.ConfiguratorFactory {
    @Override
    public CompactionJobConfigurator createConfigurator(State state) throws IOException {
      return new CompactionAvroJobConfigurator(state);
    }
  }

  @Override
  public String getFileExtension(){
    return EXTENSION.AVRO.getExtensionString();
  }

  /**
   * Constructor
   * @param  state  A task level state
   */
  public CompactionAvroJobConfigurator(State state) throws IOException {
    super(state);
    keyFieldBlacklist =
        Optional.fromNullable(state.getProp(MRCompactorAvroKeyDedupJobRunner.COMPACTION_JOB_KEY_FIELD_BLACKLIST));
  }

  /**
   * Refer to MRCompactorAvroKeyDedupJobRunner#getDedupKeyOption()
   */
  private MRCompactorAvroKeyDedupJobRunner.DedupKeyOption getDedupKeyOption() {
    if (!this.state.contains(MRCompactorAvroKeyDedupJobRunner.COMPACTION_JOB_DEDUP_KEY)) {
      return MRCompactorAvroKeyDedupJobRunner.DEFAULT_DEDUP_KEY_OPTION;
    }
    Optional<MRCompactorAvroKeyDedupJobRunner.DedupKeyOption> option =
        Enums.getIfPresent(MRCompactorAvroKeyDedupJobRunner.DedupKeyOption.class,
            this.state.getProp(MRCompactorAvroKeyDedupJobRunner.COMPACTION_JOB_DEDUP_KEY).toUpperCase());
    return option.isPresent() ? option.get() : MRCompactorAvroKeyDedupJobRunner.DEFAULT_DEDUP_KEY_OPTION;
  }

  /**
   * Refer to MRCompactorAvroKeyDedupJobRunner#getKeySchema(Job, Schema)
   */
  @VisibleForTesting
  Schema getDedupKeySchema(Schema topicSchema) {

    boolean keySchemaFileSpecified =
        this.state.contains(MRCompactorAvroKeyDedupJobRunner.COMPACTION_JOB_AVRO_KEY_SCHEMA_LOC);

    Schema keySchema = null;

    MRCompactorAvroKeyDedupJobRunner.DedupKeyOption dedupKeyOption = getDedupKeyOption();
    if (dedupKeyOption == MRCompactorAvroKeyDedupJobRunner.DedupKeyOption.ALL) {
      log.info("Using all attributes in the schema (except Map, Arrar and Enum fields) for compaction");
      keySchema = AvroUtils.removeUncomparableFields(topicSchema).get();
    } else if (dedupKeyOption == MRCompactorAvroKeyDedupJobRunner.DedupKeyOption.KEY) {
      log.info("Using key attributes in the schema for compaction");
      keySchema = AvroUtils.removeUncomparableFields(MRCompactorAvroKeyDedupJobRunner.getKeySchema(topicSchema)).get();
    } else if (keySchemaFileSpecified) {
      Path keySchemaFile = new Path(state.getProp(MRCompactorAvroKeyDedupJobRunner.COMPACTION_JOB_AVRO_KEY_SCHEMA_LOC));
      log.info("Using attributes specified in schema file " + keySchemaFile + " for compaction");
      try {
        keySchema = AvroUtils.parseSchemaFromFile(keySchemaFile, this.fs);
      } catch (IOException e) {
        log.error("Failed to parse avro schema from " + keySchemaFile
            + ", using key attributes in the schema for compaction");
        keySchema =
            AvroUtils.removeUncomparableFields(MRCompactorAvroKeyDedupJobRunner.getKeySchema(topicSchema)).get();
      }

      if (!MRCompactorAvroKeyDedupJobRunner.isKeySchemaValid(keySchema, topicSchema)) {
        log.warn(String.format("Key schema %s is not compatible with record schema %s.", keySchema, topicSchema)
            + "Using key attributes in the schema for compaction");
        keySchema =
            AvroUtils.removeUncomparableFields(MRCompactorAvroKeyDedupJobRunner.getKeySchema(topicSchema)).get();
      }
    } else {
      log.info("Property " + MRCompactorAvroKeyDedupJobRunner.COMPACTION_JOB_AVRO_KEY_SCHEMA_LOC
          + " not provided. Using key attributes in the schema for compaction");
      keySchema = AvroUtils.removeUncomparableFields(MRCompactorAvroKeyDedupJobRunner.getKeySchema(topicSchema)).get();
    }


    if (keyFieldBlacklist.isPresent()) {
      AvroSchemaFieldRemover fieldRemover = new AvroSchemaFieldRemover(keyFieldBlacklist.get());
      keySchema = fieldRemover.removeFields(keySchema);
      log.info("Adjusted key schema {}", keySchema);
    }

    return keySchema;
  }

  @Override
  protected void configureSchema(Job job) throws IOException {
    Schema newestSchema = MRCompactorAvroKeyDedupJobRunner.getNewestSchemaFromSource(job, this.fs);
    if (newestSchema != null) {
      if (this.state.getPropAsBoolean(MRCompactorAvroKeyDedupJobRunner.COMPACTION_JOB_AVRO_SINGLE_INPUT_SCHEMA, true)) {
        AvroJob.setInputKeySchema(job, newestSchema);
      }
      AvroJob.setMapOutputKeySchema(job, this.shouldDeduplicate ? getDedupKeySchema(newestSchema) : newestSchema);
      AvroJob.setMapOutputValueSchema(job, newestSchema);
      AvroJob.setOutputKeySchema(job, newestSchema);
    }
  }

  protected void configureMapper(Job job) {
    job.setInputFormatClass(AvroKeyRecursiveCombineFileInputFormat.class);
    job.setMapperClass(AvroKeyMapper.class);
    job.setMapOutputKeyClass(AvroKey.class);
    job.setMapOutputValueClass(AvroValue.class);
  }

  protected void configureReducer(Job job) throws IOException {
    job.setOutputFormatClass(AvroKeyCompactorOutputFormat.class);
    job.setReducerClass(AvroKeyDedupReducer.class);
    job.setOutputKeyClass(AvroKey.class);
    job.setOutputValueClass(NullWritable.class);
    setNumberOfReducers(job);
  }
}


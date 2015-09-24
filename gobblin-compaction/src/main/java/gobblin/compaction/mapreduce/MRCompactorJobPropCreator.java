/*
 *
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.compaction.mapreduce;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;


/**
 * This class creates the following properties for a single MapReduce job for compaction:
 * compaction.topic, compaction.job.input.dir, compaction.job.dest.dir, compaction.job.dest.dir.
 *
 * @author ziliu
 */
public class MRCompactorJobPropCreator {
  private static final Logger LOG = LoggerFactory.getLogger(MRCompactorJobPropCreator.class);

  @SuppressWarnings("unchecked")
  static class Builder<T extends Builder<?>> {

    String topic;
    Path topicInputDir;
    Path topicOutputDir;
    Path topicTmpDir;
    FileSystem fs;
    boolean deduplicate;
    State state;

    T withTopic(String topic) {
      this.topic = topic;
      return (T) this;
    }

    T withTopicInputDir(Path topicInputDir) {
      this.topicInputDir = topicInputDir;
      return (T) this;
    }

    T withTopicOutputDir(Path topicOutputDir) {
      this.topicOutputDir = topicOutputDir;
      return (T) this;
    }

    T withTopicTmpDir(Path topicTmpDir) {
      this.topicTmpDir = topicTmpDir;
      return (T) this;
    }

    T withFileSystem(FileSystem fs) {
      this.fs = fs;
      return (T) this;
    }

    T withDeduplicate(boolean deduplicate) {
      this.deduplicate = deduplicate;
      return (T) this;
    }

    T withState(State state) {
      this.state = new State();
      this.state.addAll(state);
      return (T) this;
    }

    MRCompactorJobPropCreator build() {
      return new MRCompactorJobPropCreator(this);
    }
  }

  protected final String topic;
  protected final Path topicInputDir;
  protected final Path topicOutputDir;
  protected final Path topicTmpDir;
  protected final FileSystem fs;
  protected final boolean deduplicate;
  protected final State state;

  protected MRCompactorJobPropCreator(Builder<?> builder) {
    this.topic = builder.topic;
    this.topicInputDir = builder.topicInputDir;
    this.topicOutputDir = builder.topicOutputDir;
    this.topicTmpDir = builder.topicTmpDir;
    this.fs = builder.fs;
    this.deduplicate = builder.deduplicate;
    this.state = builder.state;
  }

  protected List<State> createJobProps() throws IOException {
    List<State> emptyJobProps = Lists.newArrayList();
    if (!this.fs.exists(this.topicInputDir)) {
      LOG.warn("Input folder " + this.topicInputDir + " does not exist. Skipping topic " + this.topic);
      return emptyJobProps;
    }
    return Collections.singletonList(
        createJobProps(this.topicInputDir, this.topicOutputDir, this.topicTmpDir, this.deduplicate));
  }

  /**
   * Create MR job properties for a specific input folder and output folder.
   */
  protected State createJobProps(Path jobInputDir, Path jobOutputDir, Path jobTmpDir, boolean deduplicate)
      throws IOException {
    State jobProps = new State();
    jobProps.addAll(this.state);
    jobProps.setProp(ConfigurationKeys.COMPACTION_TOPIC, this.topic);
    jobProps.setProp(ConfigurationKeys.COMPACTION_JOB_INPUT_DIR, jobInputDir);
    jobProps.setProp(ConfigurationKeys.COMPACTION_JOB_DEST_DIR, jobOutputDir);
    jobProps.setProp(ConfigurationKeys.COMPACTION_JOB_TMP_DIR, jobTmpDir);
    jobProps.setProp(ConfigurationKeys.COMPACTION_ENABLE_SUCCESS_FILE, false);
    jobProps.setProp(ConfigurationKeys.COMPACTION_DEDUPLICATE, deduplicate);
    LOG.info(String.format("Created MR job properties for input %s and output %s.", jobInputDir, jobOutputDir));
    return jobProps;
  }

  /**
   * Create MR job properties for a specific input folder ,output folder and partition
   */
  protected State createJobProps(Path jobInputDir, Path jobOutputDir, Path jobTmpDir, boolean deduplicate, String partition)
      throws IOException {
    State jobProps = createJobProps(jobInputDir, jobOutputDir, jobTmpDir, deduplicate);
    jobProps.setProp(ConfigurationKeys.COMPACTION_JOB_DEST_PARTITION, partition);
    return jobProps;
  }
}

/* (c) 2015 LinkedIn Corp. All rights reserved.
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

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

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
  protected final State state;

  protected MRCompactorJobPropCreator(Builder<?> builder) {
    this.topic = builder.topic;
    this.topicInputDir = builder.topicInputDir;
    this.topicOutputDir = builder.topicOutputDir;
    this.topicTmpDir = builder.topicTmpDir;
    this.fs = builder.fs;
    this.state = builder.state;
  }

  protected List<State> createJobProps() throws IOException {
    List<State> emptyJobProps = Lists.newArrayList();
    if (!fs.exists(this.topicInputDir)) {
      LOG.warn("Input folder " + this.topicInputDir + " does not exist. Skipping topic " + topic);
      return emptyJobProps;
    }
    Optional<State> jobProps = createJobProps(this.topicInputDir, this.topicOutputDir, this.topicTmpDir);
    if (jobProps.isPresent()) {
      return Collections.singletonList(jobProps.get());
    } else {
      return emptyJobProps;
    }
  }

  /**
   * Create MR job properties for a specific input folder and output folder.
   * @return an Optional&lt;State&gt; object. If the input folder should not
   * be processed (e.g., jobInputDir already exists, and force reprocess is 
   * set to false), the returned object should be absent.
   */
  protected Optional<State> createJobProps(Path jobInputDir, Path jobOutputDir, Path jobTmpDir) throws IOException {
    State jobProps = new State();
    jobProps.addAll(this.state);
    jobProps.setProp(ConfigurationKeys.COMPACTION_TOPIC, this.topic);
    jobProps.setProp(ConfigurationKeys.COMPACTION_JOB_INPUT_DIR, jobInputDir);
    jobProps.setProp(ConfigurationKeys.COMPACTION_JOB_DEST_DIR, jobOutputDir);
    jobProps.setProp(ConfigurationKeys.COMPACTION_JOB_TMP_DIR, jobTmpDir);
    LOG.info(String.format("Created MR job properties for input %s and output %s.", jobInputDir, jobOutputDir));
    if (!fs.exists(jobOutputDir)) {
      return Optional.of(jobProps);
    } else if (forceReprocess()) {
      LOG.info(String.format("Outout dir %s for topic %s exists, force reprocess = true. Reprocessing.", topic,
          jobOutputDir));
      return Optional.of(jobProps);
    } else {
      LOG.info(String.format("Outout dir %s for topic %s exists, force reprocess = false. Skipping.", topic,
          jobOutputDir));
      return Optional.absent();
    }
  }

  private boolean forceReprocess() {
    return this.state.getPropAsBoolean(ConfigurationKeys.COMPACTION_FORCE_REPROCESS, false);
  }
}

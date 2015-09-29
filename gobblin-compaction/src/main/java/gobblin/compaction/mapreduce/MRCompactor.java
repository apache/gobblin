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
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import gobblin.compaction.Compactor;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.metrics.GobblinMetrics;
import gobblin.util.DatasetFilterUtils;
import gobblin.util.HadoopUtils;


/**
 * MapReduce-based {@link gobblin.compaction.Compactor}.
 *
 * @author ziliu
 */
public class MRCompactor implements Compactor {

  private static final Logger LOG = LoggerFactory.getLogger(MRCompactor.class);
  private static final List<Job> RUNNING_MR_JOBS = Lists.newCopyOnWriteArrayList();

  private final State state;
  private final Configuration conf;
  private final String inputDir;
  private final String inputSubDir;
  private final String destDir;
  private final String destSubDir;
  private final String tmpDir;
  private final FileSystem fs;
  private final ExecutorService executorService;
  private final List<Future<?>> futures;
  private final GobblinMetrics gobblinMetrics;

  public MRCompactor(Properties props) throws IOException {
    this.state = new State();
    state.addAll(props);
    this.conf = HadoopUtils.getConfFromState(state);
    this.inputDir = getInputDir();
    this.inputSubDir = getInputSubDir();
    this.destDir = getDestDir();
    this.destSubDir = getDestSubDir();
    this.tmpDir = getTmpDir();
    this.fs = getFileSystem();
    this.executorService = createExecutorService();
    this.futures = Lists.newArrayList();
    this.gobblinMetrics = initializeMetrics();
  }

  private String getInputDir() {
    Preconditions.checkArgument(this.state.contains(ConfigurationKeys.COMPACTION_INPUT_DIR),
        "Property " + ConfigurationKeys.COMPACTION_INPUT_DIR + " not provided");
    return this.state.getProp(ConfigurationKeys.COMPACTION_INPUT_DIR);
  }

  private String getInputSubDir() {
    return this.state.getProp(ConfigurationKeys.COMPACTION_INPUT_SUBDIR,
        ConfigurationKeys.DEFAULT_COMPACTION_INPUT_SUBDIR);
  }

  private String getDestDir() {
    Preconditions.checkArgument(this.state.contains(ConfigurationKeys.COMPACTION_DEST_DIR),
        "Property " + ConfigurationKeys.COMPACTION_DEST_DIR + " not provided");
    return this.state.getProp(ConfigurationKeys.COMPACTION_DEST_DIR);
  }

  private String getDestSubDir() {
    return this.state.getProp(ConfigurationKeys.COMPACTION_DEST_SUBDIR,
        ConfigurationKeys.DEFAULT_COMPACTION_DEST_SUBDIR);
  }

  private String getTmpDir() {
    return this.state.getProp(ConfigurationKeys.COMPACTION_TMP_DIR, ConfigurationKeys.DEFAULT_COMPACTION_TMP_DIR);
  }

  private FileSystem getFileSystem() throws IOException {
    URI uri =
        URI.create(this.state.getProp(ConfigurationKeys.COMPACTION_FILE_SYSTEM_URI, ConfigurationKeys.LOCAL_FS_URI));
    return FileSystem.get(uri, this.conf);
  }

  private ExecutorService createExecutorService() {
    int threadPoolSize = getThreadPoolSize();
    return Executors.newFixedThreadPool(threadPoolSize);
  }

  private int getThreadPoolSize() {
    return this.state.getPropAsInt(ConfigurationKeys.COMPACTION_THREAD_POOL_SIZE,
        ConfigurationKeys.DEFAULT_COMPACTION_THREAD_POOL_SIZE);
  }

  @Override
  public void compact() throws IOException {
    try {
      copyDependencyJarsToHdfs();
      Set<String> topics = findAllTopics();
      processTopics(topics);

    } finally {
      this.executorService.shutdown();
      try {
        executorService.awaitTermination(getMRJobTimeoutValue(), TimeUnit.MINUTES);
        deleteDependencyJars();
        for (Future<?> future : this.futures) {

          // The purpose of calling future.get() is to throw whatever exceptions
          // thrown by the corresponding thread. Otherwise those exceptions
          // will be eaten.
          future.get();
        }

        gobblinMetrics.stopMetricReporting();

      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for Hadoop jobs to complete", e);
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        LOG.warn("Got an Exception while processing an input folder", e);
        throw new RuntimeException(e);
      }
    }
  }

  private GobblinMetrics initializeMetrics() {
    GobblinMetrics gobblinMetrics = GobblinMetrics.get(state.getProp(ConfigurationKeys.JOB_NAME_KEY));
    gobblinMetrics.startMetricReporting(state.getProperties());
    return gobblinMetrics;
  }

  /**
   * Copy dependency jars from local fs to HDFS.
   */
  private void copyDependencyJarsToHdfs() throws IOException {
    if (!this.state.contains(ConfigurationKeys.JOB_JAR_FILES_KEY)) {
      return;
    }
    LocalFileSystem lfs = FileSystem.getLocal(this.conf);
    Path tmpJarFileDir = new Path(this.inputDir, "_gobblin_compaction_jars");
    this.state.setProp(ConfigurationKeys.COMPACTION_JARS, tmpJarFileDir.toString());
    this.fs.delete(tmpJarFileDir, true);
    for (String jarFile : this.state.getPropAsList(ConfigurationKeys.JOB_JAR_FILES_KEY)) {
      for (FileStatus status : lfs.globStatus(new Path(jarFile))) {
        Path tmpJarFile = new Path(this.fs.makeQualified(tmpJarFileDir), status.getPath().getName());
        this.fs.copyFromLocalFile(status.getPath(), tmpJarFile);
        LOG.info(String.format("%s will be added to classpath", tmpJarFile));
      }
    }
  }

  /**
   * Delete dependency jars from HDFS when job is done.
   */
  private void deleteDependencyJars() throws IllegalArgumentException, IOException {
    if (this.state.contains(ConfigurationKeys.COMPACTION_JARS)) {
      this.fs.delete(new Path(this.state.getProp(ConfigurationKeys.COMPACTION_JARS)), true);
    }
  }

  /**
   * Each subdir name in this.inputDir is considered a topic, if it satisfies
   * blacklist and whitelist.
   */
  @SuppressWarnings("deprecation")
  private Set<String> findAllTopics() throws IOException {
    Set<String> topics = Sets.newHashSet();
    List<Pattern> blacklist = getBlacklist();
    List<Pattern> whitelist = getWhitelist();
    for (FileStatus status : this.fs.listStatus(new Path(this.inputDir))) {
      if (status.isDir()) {
        String topic = status.getPath().getName();
        if (DatasetFilterUtils.survived(topic, blacklist, whitelist)) {
          LOG.info("found topic: " + status.getPath().getName());
          topics.add(status.getPath().getName());
        }
      }
    }
    return topics;
  }

  private List<Pattern> getBlacklist() {
    List<String> list = this.state.getPropAsList(ConfigurationKeys.COMPACTION_BLACKLIST, StringUtils.EMPTY);
    return DatasetFilterUtils.getPatternsFromStrings(list);
  }

  private List<Pattern> getWhitelist() {
    List<String> list = this.state.getPropAsList(ConfigurationKeys.COMPACTION_WHITELIST, StringUtils.EMPTY);
    return DatasetFilterUtils.getPatternsFromStrings(list);
  }

  private void processTopics(Set<String> topics) throws IOException {
    processPriorityTopics(topics, getHighPriorityTopics());
    processPriorityTopics(topics, getNormalPriorityTopics());
    processRemainingTopics(topics);
  }

  private void processPriorityTopics(Set<String> topics, List<Pattern> priorityTopics) throws IOException {
    for (Iterator<String> it = topics.iterator(); it.hasNext();) {
      String topic = it.next();
      if (DatasetFilterUtils.stringInPatterns(topic, priorityTopics)) {
        processTopic(topic);

        // Remove the topic from topics to avoid processing it again.
        it.remove();
      }
    }
  }

  private void processRemainingTopics(Set<String> topics) throws IOException {
    for (String topic : topics) {
      processTopic(topic);
    }
  }

  private List<Pattern> getHighPriorityTopics() {
    List<String> list = this.state.getPropAsList(ConfigurationKeys.COMPACTION_HIGH_PRIORITY_TOPICS, StringUtils.EMPTY);
    return DatasetFilterUtils.getPatternsFromStrings(list);
  }

  private List<Pattern> getNormalPriorityTopics() {
    List<String> list =
        this.state.getPropAsList(ConfigurationKeys.COMPACTION_NORMAL_PRIORITY_TOPICS, StringUtils.EMPTY);
    return DatasetFilterUtils.getPatternsFromStrings(list);
  }

  private void processTopic(String topic) throws IOException {
    LOG.info("Creating MR jobs for topic " + topic);
    Path topicSourcePath = new Path(this.inputDir, new Path(topic, this.inputSubDir));
    Path topicDestPath = new Path(this.destDir, new Path(topic, this.destSubDir));
    Path topicTmpPath = new Path(this.tmpDir, new Path(topic, this.destSubDir));
    MRCompactorJobPropCreator jobPropCreator = getJobPropCreator(topic, topicSourcePath, topicDestPath, topicTmpPath);
    List<State> allJobProps = jobPropCreator.createJobProps();
    for (State jobProps : allJobProps) {
      processJob(jobProps);
    }
  }

  /**
   * Get an instance of MRCompactorJobPropCreator, e.g., MRCompactorTimeBasedJobPropCreator for time-based compaction.
   */
  MRCompactorJobPropCreator getJobPropCreator(String topic, Path topicInputDir, Path topicOutputDir, Path topicTmpDir) {
    String builderClassName = this.state.getProp(ConfigurationKeys.COMPACTION_JOBPROPS_CREATOR_CLASS,
        ConfigurationKeys.DEFAULT_COMPACTION_JOBPROPS_CREATOR_CLASS) + "$Builder";
    boolean deduplicate = this.state.getPropAsBoolean(ConfigurationKeys.COMPACTION_DEDUPLICATE,
        ConfigurationKeys.DEFAULT_COMPACTION_DEDUPLICATE);

    try {
      return ((MRCompactorJobPropCreator.Builder<?>) Class.forName(builderClassName).newInstance()).withTopic(topic)
          .withTopicInputDir(topicInputDir).withTopicOutputDir(topicOutputDir).withTopicTmpDir(topicTmpDir)
          .withFileSystem(this.fs).withDeduplicate(deduplicate).withState(this.state).build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void processJob(State jobProps) {
    this.futures.add(this.executorService.submit(getMRCompactorJobRunner(jobProps)));
  }

  /**
   * Get an instance of MRCompactorJobRunner, e.g., MRCompactorAvroKeyJobRunner if the data is in Avro format.
   */
  private MRCompactorJobRunner getMRCompactorJobRunner(State jobProps) {
    try {
      @SuppressWarnings("unchecked")
      Class<? extends MRCompactorJobRunner> cls =
          (Class<? extends MRCompactorJobRunner>) Class.forName(jobProps.getProp(
              ConfigurationKeys.COMPACTION_JOB_RUNNER_CLASS, ConfigurationKeys.DEFAULT_COMPACTION_JOB_RUNNER_CLASS));
      return cls.getDeclaredConstructor(State.class, FileSystem.class).newInstance(jobProps, this.fs);
    } catch (Exception e) {
      throw new RuntimeException("Cannot instantiate MRCompactorJobRunner", e);
    }
  }

  public static void addRunningHadoopJob(Job job) {
    MRCompactor.RUNNING_MR_JOBS.add(job);
  }

  private long getMRJobTimeoutValue() {
    return this.state.getPropAsLong(ConfigurationKeys.COMPACTION_MR_JOB_TIMEOUT_MINUTES,
        ConfigurationKeys.DEFAULT_COMPACTION_MR_JOB_TIMEOUT_MINUTES);
  }

  public void cancel() throws IOException {
    this.executorService.shutdownNow();
    for (Job hadoopJob : MRCompactor.RUNNING_MR_JOBS) {
      if (!hadoopJob.isComplete()) {
        hadoopJob.killJob();
      }
    }
  }
}

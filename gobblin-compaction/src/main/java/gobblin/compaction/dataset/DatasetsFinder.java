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

package gobblin.compaction.dataset;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

import gobblin.compaction.mapreduce.MRCompactor;
import gobblin.configuration.State;
import gobblin.util.DatasetFilterUtils;
import gobblin.util.HadoopUtils;


/**
 * {@link Dataset}s finder to identify datasets, using given properties.
 */
@Slf4j
public abstract class DatasetsFinder implements gobblin.dataset.DatasetsFinder<Dataset> {
  public static final double HIGH_PRIORITY = 3.0;
  public static final double NORMAL_PRIORITY = 2.0;
  public static final double LOW_PRIORITY = 1.0;
  public static final char DATASETS_WITH_DIFFERENT_RECOMPACT_THRESHOLDS_SEPARATOR = ';';
  public static final char DATASETS_WITH_SAME_RECOMPACT_THRESHOLDS_SEPARATOR = ',';
  public static final char DATASETS_AND_RECOMPACT_THRESHOLD_SEPARATOR = ':';

  protected final State state;
  protected final Configuration conf;
  protected final FileSystem fs;
  protected final String inputDir;
  protected final String destDir;
  protected final String tmpOutputDir;
  protected final List<Pattern> blacklist;
  protected final List<Pattern> whitelist;
  protected final List<Pattern> highPriority;
  protected final List<Pattern> normalPriority;
  protected final boolean recompactDatasets;

  public DatasetsFinder(State state) {
    this.state = state;
    this.conf = HadoopUtils.getConfFromState(state);
    this.fs = getFileSystem();
    this.inputDir = getInputDir();
    this.destDir = getDestDir();
    this.tmpOutputDir = getTmpOutputDir();
    this.blacklist = getBlacklist();
    this.whitelist = getWhitelist();
    this.highPriority = getHighPriorityPatterns();
    this.normalPriority = getNormalPriorityPatterns();
    this.recompactDatasets = getRecompactDatasets();
  }

  /**
   * Create a {@link Dataset}, which is comparable, using {@link #inputDir} and {@link #destDir}.
   */
  public abstract Set<Dataset> findDistinctDatasets() throws IOException;

  @Override
  public List<Dataset> findDatasets() throws IOException {
    return Lists.newArrayList(this.findDistinctDatasets());
  }

  /**
   * @return {@link #destDir} shared by all {@link Dataset}s root paths.
   */
  @Override
  public Path commonDatasetRoot() {
    return new Path(this.destDir);
  }

  private String getInputDir() {
    Preconditions.checkArgument(this.state.contains(MRCompactor.COMPACTION_INPUT_DIR), "Missing required property "
        + MRCompactor.COMPACTION_INPUT_DIR);
    return this.state.getProp(MRCompactor.COMPACTION_INPUT_DIR);
  }

  private String getDestDir() {
    Preconditions.checkArgument(this.state.contains(MRCompactor.COMPACTION_DEST_DIR), "Missing required property "
        + MRCompactor.COMPACTION_DEST_DIR);
    return this.state.getProp(MRCompactor.COMPACTION_DEST_DIR);
  }

  private String getTmpOutputDir() {
    return this.state.getProp(MRCompactor.COMPACTION_TMP_DEST_DIR, MRCompactor.DEFAULT_COMPACTION_TMP_DEST_DIR);
  }

  private FileSystem getFileSystem() {
    try {
      if (this.state.contains(MRCompactor.COMPACTION_FILE_SYSTEM_URI)) {
        URI uri = URI.create(this.state.getProp(MRCompactor.COMPACTION_FILE_SYSTEM_URI));
        return FileSystem.get(uri, this.conf);
      } else {
        return FileSystem.get(this.conf);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to get filesystem for datasetsFinder.", e);
    }
  }

  private List<Pattern> getBlacklist() {
    List<String> list = this.state.getPropAsList(MRCompactor.COMPACTION_BLACKLIST, StringUtils.EMPTY);
    return DatasetFilterUtils.getPatternsFromStrings(list);
  }

  private List<Pattern> getWhitelist() {
    List<String> list = this.state.getPropAsList(MRCompactor.COMPACTION_WHITELIST, StringUtils.EMPTY);
    return DatasetFilterUtils.getPatternsFromStrings(list);
  }

  private List<Pattern> getHighPriorityPatterns() {
    List<String> list = this.state.getPropAsList(MRCompactor.COMPACTION_HIGH_PRIORITY_TOPICS, StringUtils.EMPTY);
    return DatasetFilterUtils.getPatternsFromStrings(list);
  }

  private List<Pattern> getNormalPriorityPatterns() {
    List<String> list = this.state.getPropAsList(MRCompactor.COMPACTION_NORMAL_PRIORITY_TOPICS, StringUtils.EMPTY);
    return DatasetFilterUtils.getPatternsFromStrings(list);
  }

  private boolean getRecompactDatasets() {
    return this.state.getPropAsBoolean(MRCompactor.COMPACTION_RECOMPACT_FROM_DEST_PATHS,
        MRCompactor.DEFAULT_COMPACTION_RECOMPACT_FROM_DEST_PATHS);
  }

  protected double getDatasetPriority(String datasetName) {
    double priority = LOW_PRIORITY;
    if (DatasetFilterUtils.stringInPatterns(datasetName, this.highPriority)) {
      priority = HIGH_PRIORITY;
    } else if (DatasetFilterUtils.stringInPatterns(datasetName, this.normalPriority)) {
      priority = NORMAL_PRIORITY;
    }
    return priority;
  }

  private Map<String, Double> getDatasetRegexAndRecompactThreshold(String datasetsAndRecompactThresholds) {
    Map<String, Double> topicRegexAndRecompactThreshold = Maps.newHashMap();
    for (String entry : Splitter.on(DATASETS_WITH_DIFFERENT_RECOMPACT_THRESHOLDS_SEPARATOR).trimResults()
        .omitEmptyStrings().splitToList(datasetsAndRecompactThresholds)) {
      List<String> topicsAndRecompactThreshold =
          Splitter.on(DATASETS_AND_RECOMPACT_THRESHOLD_SEPARATOR).trimResults().omitEmptyStrings()
              .splitToList(entry);
      if (topicsAndRecompactThreshold.size() != 2) {
        log.error("Invalid form (DATASET_NAME:THRESHOLD) in "
            + MRCompactor.COMPACTION_LATEDATA_THRESHOLD_FOR_RECOMPACT_PER_DATASET + ".");
      } else {
        topicRegexAndRecompactThreshold.put(topicsAndRecompactThreshold.get(0),
            Double.parseDouble(topicsAndRecompactThreshold.get(1)));
      }
    }
    return topicRegexAndRecompactThreshold;
  }

  protected double getDatasetRecompactThreshold(String datasetName) {
    Map<String, Double> datasetRegexAndRecompactThreshold =
        this.getDatasetRegexAndRecompactThreshold(this.state.getProp(
            MRCompactor.COMPACTION_LATEDATA_THRESHOLD_FOR_RECOMPACT_PER_DATASET, StringUtils.EMPTY));
    for (Map.Entry<String, Double> topicRegexEntry : datasetRegexAndRecompactThreshold.entrySet()) {
      if (DatasetFilterUtils.stringInPatterns(
          datasetName,
          DatasetFilterUtils.getPatternsFromStrings(Splitter.on(DATASETS_WITH_SAME_RECOMPACT_THRESHOLDS_SEPARATOR)
              .trimResults().omitEmptyStrings().splitToList(topicRegexEntry.getKey())))) {
        return topicRegexEntry.getValue();
      }
    }
    return MRCompactor.DEFAULT_COMPACTION_LATEDATA_THRESHOLD_FOR_RECOMPACT_PER_DATASET;
  }
}

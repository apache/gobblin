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

package gobblin.compaction.dataset;

import gobblin.source.extractor.extract.kafka.ConfigStoreUtils;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import gobblin.compaction.mapreduce.MRCompactor;
import gobblin.configuration.State;
import gobblin.util.DatasetFilterUtils;
import gobblin.util.HadoopUtils;


/**
 * {@link Dataset}s finder to identify datasets, using given properties.
 */
public abstract class DatasetsFinder implements gobblin.dataset.DatasetsFinder<Dataset> {
  public static final double HIGH_PRIORITY = 3.0;
  public static final double NORMAL_PRIORITY = 2.0;
  public static final double LOW_PRIORITY = 1.0;
  public static final String TMP_OUTPUT_SUBDIR = "output";

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
    this(state, getFileSystem(state));
  }

  @VisibleForTesting
  DatasetsFinder(State state, FileSystem fs) {
    this.state = state;
    this.conf = HadoopUtils.getConfFromState(state);
    this.fs = fs;
    this.inputDir = getInputDir();
    this.destDir = getDestDir();
    this.tmpOutputDir = getTmpOutputDir();
    this.blacklist = DatasetFilterUtils.getPatternList(state, MRCompactor.COMPACTION_BLACKLIST);
    this.whitelist = DatasetFilterUtils.getPatternList(state, MRCompactor.COMPACTION_WHITELIST);
    setTopicsFromConfigStore(state);
    this.highPriority = getHighPriorityPatterns();
    this.normalPriority = getNormalPriorityPatterns();
    this.recompactDatasets = getRecompactDatasets();
  }

  private void setTopicsFromConfigStore(State state) {
    Set<String> blacklistTopicsFromConfigStore = new HashSet<>();
    Set<String> whitelistTopicsFromConfigStore = new HashSet<>();
    ConfigStoreUtils.setTopicsFromConfigStore(state.getProperties(), blacklistTopicsFromConfigStore,
        whitelistTopicsFromConfigStore, MRCompactor.COMPACTION_BLACKLIST, MRCompactor.COMPACTION_WHITELIST);
    this.blacklist.addAll(DatasetFilterUtils.getPatternsFromStrings(new ArrayList<>(blacklistTopicsFromConfigStore)));
    this.whitelist.addAll(DatasetFilterUtils.getPatternsFromStrings(new ArrayList<>(whitelistTopicsFromConfigStore)));
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
    Preconditions.checkArgument(this.state.contains(MRCompactor.COMPACTION_INPUT_DIR),
        "Missing required property " + MRCompactor.COMPACTION_INPUT_DIR);
    return this.state.getProp(MRCompactor.COMPACTION_INPUT_DIR);
  }

  private String getDestDir() {
    Preconditions.checkArgument(this.state.contains(MRCompactor.COMPACTION_DEST_DIR),
        "Missing required property " + MRCompactor.COMPACTION_DEST_DIR);
    return this.state.getProp(MRCompactor.COMPACTION_DEST_DIR);
  }

  private String getTmpOutputDir() {
    return new Path(this.state.getProp(MRCompactor.COMPACTION_TMP_DEST_DIR,
        MRCompactor.DEFAULT_COMPACTION_TMP_DEST_DIR), TMP_OUTPUT_SUBDIR).toString();
  }

  private static FileSystem getFileSystem(State state) {
    try {
      if (state.contains(MRCompactor.COMPACTION_FILE_SYSTEM_URI)) {
        URI uri = URI.create(state.getProp(MRCompactor.COMPACTION_FILE_SYSTEM_URI));
        return FileSystem.get(uri, HadoopUtils.getConfFromState(state));
      }
      return FileSystem.get(HadoopUtils.getConfFromState(state));
    } catch (IOException e) {
      throw new RuntimeException("Failed to get filesystem for datasetsFinder.", e);
    }
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

}

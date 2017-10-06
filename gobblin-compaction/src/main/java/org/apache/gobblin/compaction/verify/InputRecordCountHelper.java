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
package org.apache.gobblin.compaction.verify;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.apache.gobblin.compaction.dataset.DatasetHelper;
import org.apache.gobblin.compaction.event.CompactionSlaEventHelper;
import org.apache.gobblin.compaction.mapreduce.MRCompactor;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.RecordCountProvider;
import org.apache.gobblin.util.recordcount.IngestionRecordCountProvider;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Collection;

/**
 * A class helps to calculate, serialize, deserialize record count.
 *
 * By using {@link IngestionRecordCountProvider}, the default input file name should be in format
 * {file_name}.{record_count}.{extension}. For example, given a file path: "/a/b/c/file.123.avro",
 * the record count will be 123.
 */
@Slf4j
public class InputRecordCountHelper {

  @Getter
  private final FileSystem fs;
  private final State state;
  private final RecordCountProvider inputRecordCountProvider;
  private final String AVRO = "avro";

  @Deprecated
  public final static String RECORD_COUNT_FILE = "_record_count";

  public final static String STATE_FILE = "_state_file";

  /**
   * Constructor
   */
  public InputRecordCountHelper(State state) {
    try {
      this.fs = getSourceFileSystem (state);
      this.state = state;
      this.inputRecordCountProvider = (RecordCountProvider) Class
              .forName(state.getProp(MRCompactor.COMPACTION_INPUT_RECORD_COUNT_PROVIDER,
                      MRCompactor.DEFAULT_COMPACTION_INPUT_RECORD_COUNT_PROVIDER))
              .newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate " + InputRecordCountHelper.class.getName(), e);
    }
  }

  /**
   * Calculate record count at given paths
   * @param  paths all paths where the record count are calculated
   * @return record count after parsing all files under given paths
   */
  public long calculateRecordCount (Collection<Path> paths) throws IOException {
    long sum = 0;
    for (Path path: paths) {
      sum += inputRecordCountProvider.getRecordCount(DatasetHelper.getApplicableFilePaths(this.fs, path, Lists.newArrayList(AVRO)));
    }
    return sum;
  }

  /**
   * Load compaction state file
   */
  public State loadState (Path dir) throws IOException {
    return loadState(this.fs, dir);
  }

  private static State loadState (FileSystem fs, Path dir) throws IOException {
    State state = new State();
    if (fs.exists(new Path(dir, STATE_FILE))) {
      try (FSDataInputStream inputStream = fs.open(new Path(dir, STATE_FILE))) {
        state.readFields(inputStream);
      }
    }
    return state;
  }

  /**
   * Save compaction state file
   */
  public void saveState (Path dir, State state) throws IOException {
    saveState(this.fs, dir, state);
  }

  private static void saveState (FileSystem fs, Path dir, State state) throws IOException {
    Path tmpFile = new Path(dir, STATE_FILE + ".tmp");
    Path newFile = new Path(dir, STATE_FILE);
    fs.delete(tmpFile, false);
    try (DataOutputStream dataOutputStream = new DataOutputStream(fs.create(new Path(dir, STATE_FILE + ".tmp")))) {
      state.write(dataOutputStream);
    }

    // Caution: We are deleting right before renaming because rename doesn't support atomic overwrite options from FileSystem API.
    fs.delete(newFile, false);
    fs.rename(tmpFile, newFile);
  }

  /**
   * Read record count from a specific directory.
   * File name is {@link InputRecordCountHelper#STATE_FILE}
   * @param dir directory where a state file is located
   * @return record count
   */
  public long readRecordCount (Path dir) throws IOException {
    return readRecordCount(this.fs, dir);
  }

  /**
   * Read record count from a specific directory.
   * File name is {@link InputRecordCountHelper#STATE_FILE}
   * @param fs  file system in use
   * @param dir directory where a state file is located
   * @return record count
   */
  @Deprecated
  public static long readRecordCount (FileSystem fs, Path dir) throws IOException {
    State state = loadState(fs, dir);

    if (!state.contains(CompactionSlaEventHelper.RECORD_COUNT_TOTAL)) {
      if (fs.exists(new Path (dir, RECORD_COUNT_FILE))){
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open (new Path (dir, RECORD_COUNT_FILE)), Charsets.UTF_8))) {
          long count = Long.parseLong(br.readLine());
          return count;
        }
      } else {
        return 0;
      }
    } else {
      return Long.parseLong(state.getProp(CompactionSlaEventHelper.RECORD_COUNT_TOTAL));
    }
  }

  /**
   * Read execution count from a specific directory.
   * File name is {@link InputRecordCountHelper#STATE_FILE}
   * @param dir directory where a state file is located
   * @return record count
   */
  public long readExecutionCount (Path dir) throws IOException {
    State state = loadState(fs, dir);
    return Long.parseLong(state.getProp(CompactionSlaEventHelper.EXEC_COUNT_TOTAL, "0"));
  }

  /**
   * Write record count to a specific directory.
   * File name is {@link InputRecordCountHelper#RECORD_COUNT_FILE}
   * @param fs file system in use
   * @param dir directory where a record file is located
   */
  @Deprecated
  public static void writeRecordCount (FileSystem fs, Path dir, long count) throws IOException {
     State state = loadState(fs, dir);
     state.setProp(CompactionSlaEventHelper.RECORD_COUNT_TOTAL, count);
     saveState(fs, dir, state);
  }

  protected FileSystem getSourceFileSystem (State state)
          throws IOException {
    Configuration conf = HadoopUtils.getConfFromState(state);
    String uri = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, ConfigurationKeys.LOCAL_FS_URI);
    return HadoopUtils.getOptionallyThrottledFileSystem(FileSystem.get(URI.create(uri), conf), state);
  }
}

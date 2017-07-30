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

package gobblin.source.extractor.hadoop;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.AbstractSource;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.HadoopUtils;


/**
 * An implementation of {@link gobblin.source.Source} that uses a Hadoop {@link FileInputFormat} to get a
 * {@link FileSplit} per {@link Extractor} return by {@link #getExtractor(WorkUnitState)} and a
 * {@link RecordReader} to read the {@link FileSplit}.
 *
 * <p>
 *   This class is equivalent to {@link HadoopFileInputSource} in terms of functionality except that it uses
 *   the old Hadoop API.
 * </p>
 *
 * <p>
 *   This class can read either keys of type {@link #<K>} or values of type {@link #<V>} supported by the
 *   given {@link FileInputFormat}, configurable by {@link HadoopFileInputSource#FILE_INPUT_READ_KEYS_KEY}.
 *   It will read keys if the property is set to {@code true}, otherwise it will read values. By default,
 *   it will read values through the given {@link FileInputFormat}.
 * </p>
 *
 * <p>
 *   A concrete implementation of this class should implement {@link #getFileInputFormat(State, JobConf)}
 *   and {@link #getExtractor(WorkUnitState, RecordReader, FileSplit, boolean)}, which returns a
 *   {@link OldApiHadoopFileInputExtractor} that needs an concrete implementation.
 * </p>
 *
 * @param <S> output schema type
 * @param <D> output data record type
 * @param <K> key type expected by the {@link FileInputFormat}
 * @param <V> value type expected by the {@link FileInputFormat}
 *
 * @author Yinan Li
 */
public abstract class OldApiHadoopFileInputSource<S, D, K, V> extends AbstractSource<S, D> {


  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    JobConf jobConf = new JobConf(new Configuration());
    for (String key : state.getPropertyNames()) {
      jobConf.set(key, state.getProp(key));
    }

    if (state.contains(HadoopFileInputSource.FILE_INPUT_PATHS_KEY)) {
      for (String inputPath : state.getPropAsList(HadoopFileInputSource.FILE_INPUT_PATHS_KEY)) {
        FileInputFormat.addInputPath(jobConf, new Path(inputPath));
      }
    }

    try {
      FileInputFormat<K, V> fileInputFormat = getFileInputFormat(state, jobConf);
      InputSplit[] fileSplits = fileInputFormat.getSplits(jobConf, state.getPropAsInt(
          HadoopFileInputSource.FILE_SPLITS_DESIRED_KEY, HadoopFileInputSource.DEFAULT_FILE_SPLITS_DESIRED));
      if (fileSplits == null || fileSplits.length == 0) {
        return ImmutableList.of();
      }

      Extract.TableType tableType = state.contains(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY) ?
          Extract.TableType.valueOf(state.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY).toUpperCase()) : null;
      String tableNamespace = state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY);
      String tableName = state.getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY);

      List<WorkUnit> workUnits = Lists.newArrayListWithCapacity(fileSplits.length);
      for (InputSplit inputSplit : fileSplits) {
        // Create one WorkUnit per InputSplit
        FileSplit fileSplit = (FileSplit) inputSplit;
        Extract extract = createExtract(tableType, tableNamespace, tableName);
        WorkUnit workUnit = WorkUnit.create(extract);
        workUnit.setProp(HadoopFileInputSource.FILE_SPLIT_BYTES_STRING_KEY, HadoopUtils.serializeToString(fileSplit));
        workUnit.setProp(HadoopFileInputSource.FILE_SPLIT_PATH_KEY, fileSplit.getPath().toString());
        workUnits.add(workUnit);
      }

      return workUnits;
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to get workunits", ioe);
    }
  }

  @Override
  public Extractor<S, D> getExtractor(WorkUnitState workUnitState) throws IOException {
    if (!workUnitState.contains(HadoopFileInputSource.FILE_SPLIT_BYTES_STRING_KEY)) {
      throw new IOException("No serialized FileSplit found in WorkUnitState " + workUnitState.getId());
    }

    JobConf jobConf = new JobConf(new Configuration());
    for (String key : workUnitState.getPropertyNames()) {
      jobConf.set(key, workUnitState.getProp(key));
    }

    String fileSplitBytesStr = workUnitState.getProp(HadoopFileInputSource.FILE_SPLIT_BYTES_STRING_KEY);
    FileSplit fileSplit = (FileSplit) HadoopUtils.deserializeFromString(FileSplit.class, fileSplitBytesStr);
    FileInputFormat<K, V> fileInputFormat = getFileInputFormat(workUnitState, jobConf);
    RecordReader<K, V> recordReader = fileInputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
    boolean readKeys = workUnitState.getPropAsBoolean(
        HadoopFileInputSource.FILE_INPUT_READ_KEYS_KEY, HadoopFileInputSource.DEFAULT_FILE_INPUT_READ_KEYS);
    return getExtractor(workUnitState, recordReader, fileSplit, readKeys);
  }

  @Override
  public void shutdown(SourceState state) {

  }

  /**
   * Get a {@link FileInputFormat} instance used to get {@link FileSplit}s and a {@link RecordReader}
   * for every {@link FileSplit}.
   *
   * <p>
   *   This default implementation simply creates a new instance of a {@link FileInputFormat} class
   *   specified using the configuration property {@link HadoopFileInputSource#FILE_INPUT_FORMAT_CLASS_KEY}.
   * </p>
   *
   * @param state a {@link State} object carrying configuration properties
   * @param jobConf a Hadoop {@link JobConf} object carrying Hadoop configurations
   * @return a {@link FileInputFormat} instance
   */
  @SuppressWarnings("unchecked")
  protected FileInputFormat<K, V> getFileInputFormat(State state, JobConf jobConf) {
    Preconditions.checkArgument(state.contains(HadoopFileInputSource.FILE_INPUT_FORMAT_CLASS_KEY));
    try {
      return (FileInputFormat<K, V>) ReflectionUtils.newInstance(
          Class.forName(state.getProp(HadoopFileInputSource.FILE_INPUT_FORMAT_CLASS_KEY)), new Configuration());
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException(cnfe);
    }
  }

  /**
   * Get a {@link OldApiHadoopFileInputExtractor} instance.
   *
   * @param workUnitState a {@link WorkUnitState} object carrying Gobblin configuration properties
   * @param recordReader a Hadoop {@link RecordReader} object used to read input records
   * @param fileSplit the {@link FileSplit} to read input records from
   * @param readKeys whether the {@link OldApiHadoopFileInputExtractor} should read keys of type {@link #<K>};
   *                 by default values of type {@link #>V>} are read.
   * @return a {@link OldApiHadoopFileInputExtractor} instance
   */
  protected abstract OldApiHadoopFileInputExtractor<S, D, K, V> getExtractor(WorkUnitState workUnitState,
      RecordReader<K, V> recordReader, FileSplit fileSplit, boolean readKeys);
}

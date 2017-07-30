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

package gobblin.runtime.commit;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;

import gobblin.annotation.Alpha;
import gobblin.commit.CommitSequence;
import gobblin.commit.CommitSequenceStore;
import gobblin.commit.CommitStep;
import gobblin.configuration.ConfigurationKeys;
import gobblin.util.HadoopUtils;
import gobblin.util.filters.HiddenFilter;
import gobblin.util.io.GsonInterfaceAdapter;


/**
 * An implementation of {@link CommitSequenceStore} backed by a {@link FileSystem}.
 *
 * <p>
 *    This implementation serializes a {@link CommitSequence} along with all its {@link CommitStep}s into a
 *    JSON string using {@link Gson}. Thus it requires that all {@link CommitStep}s can be serialized
 *    and deserialized with {@link Gson}.
 * </p>
 *
 * @author Ziyang Liu
 */
@Alpha
public class FsCommitSequenceStore implements CommitSequenceStore {

  public static final String GOBBLIN_RUNTIME_COMMIT_SEQUENCE_STORE_FS_URI =
      "gobblin.runtime.commit.sequence.store.fs.uri";
  public static final String GOBBLIN_RUNTIME_COMMIT_SEQUENCE_STORE_DIR = "gobblin.runtime.commit.sequence.store.dir";

  private static final String DEFAULT_DATASET_URN = "default_dataset_urn";
  private static final Gson GSON = GsonInterfaceAdapter.getGson(CommitStep.class);

  private final FileSystem fs;
  private final Path rootPath;

  public FsCommitSequenceStore(FileSystem fs, Path rootPath) {
    this.fs = fs;
    this.rootPath = rootPath;
  }

  @Override
  public boolean exists(String jobName) throws IOException {
    Path jobPath = new Path(this.rootPath, jobName);
    return this.fs.exists(jobPath);
  }

  @Override
  public boolean exists(String jobName, String datasetUrn) throws IOException {
    Path datasetPath = new Path(new Path(this.rootPath, jobName), sanitizeDatasetUrn(datasetUrn));
    return this.fs.exists(datasetPath);
  }

  @Override
  public void delete(String jobName) throws IOException {
    Path jobPath = new Path(this.rootPath, jobName);
    HadoopUtils.deletePathAndEmptyAncestors(this.fs, jobPath, true);
  }

  @Override
  public void delete(String jobName, String datasetUrn) throws IOException {
    Path jobPath = new Path(this.rootPath, jobName);
    Path datasetPath = new Path(jobPath, sanitizeDatasetUrn(datasetUrn));
    HadoopUtils.deletePathAndEmptyAncestors(this.fs, datasetPath, true);
  }

  @Override
  public void put(String jobName, String datasetUrn, CommitSequence commitSequence) throws IOException {
    datasetUrn = sanitizeDatasetUrn(datasetUrn);
    if (exists(jobName, datasetUrn)) {
      throw new IOException(String.format("CommitSequence already exists for job %s, dataset %s", jobName, datasetUrn));
    }

    Path jobPath = new Path(this.rootPath, jobName);
    this.fs.mkdirs(jobPath);

    Path datasetPath = new Path(jobPath, datasetUrn);
    try (DataOutputStream dos = this.fs.create(datasetPath)) {
      dos.writeBytes(GSON.toJson(commitSequence));
    }
  }

  @Override
  public Collection<String> get(String jobName) throws IOException {
    ImmutableList.Builder<String> builder = new ImmutableList.Builder<>();
    Path jobPath = new Path(this.rootPath, jobName);
    if (this.fs.exists(jobPath)) {
      for (FileStatus status : this.fs.listStatus(jobPath, new HiddenFilter())) {
        builder.add(status.getPath().getName());
      }
    }
    return builder.build();
  }

  @Override
  public Optional<CommitSequence> get(String jobName, String datasetUrn) throws IOException {
    if (!exists(jobName, datasetUrn)) {
      return Optional.<CommitSequence> absent();
    }

    Path datasetPath = new Path(new Path(this.rootPath, jobName), sanitizeDatasetUrn(datasetUrn));
    try (InputStream is = this.fs.open(datasetPath)) {
      return Optional
          .of(GSON.fromJson(IOUtils.toString(is, ConfigurationKeys.DEFAULT_CHARSET_ENCODING), CommitSequence.class));
    }
  }

  /**
   * Replace a null or empty dataset URN with {@link #DEFAULT_DATASET_URN}, and replaces illegal HDFS
   * characters with '_'.
   */
  private static String sanitizeDatasetUrn(String datasetUrn) {
    return Strings.isNullOrEmpty(datasetUrn) ? DEFAULT_DATASET_URN : HadoopUtils.sanitizePath(datasetUrn, "_");
  }

}

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

package org.apache.gobblin.data.management.copy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.data.management.retention.profile.ConfigurableGlobDatasetFinder;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.util.filters.RegexPathFilter;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;


/**
 * {@link SubsetFilesCopyableDataset} finder that extends {@link ConfigurableGlobDatasetFinder}.
 *
 * It lists files on the root path, and uses a user defined pattern to identify {@link SubsetFilesCopyableDataset}s and
 * their corresponding subset of files.
 */
@Slf4j
public class SubsetFilesCopyableDatasetFinder extends ConfigurableGlobDatasetFinder<CopyableDataset> {
  public static final String IDENTIFIER_PATTERN = CopyConfiguration.COPY_PREFIX + ".subsetFilesDatasetIdentifer";
  public static final String DEFAULT_IDENTIFIER_PATTERN = "(.*)";

  public static final String SUBSETFILES_REGEX_FILTER = CopyConfiguration.COPY_PREFIX + ".subsetFilesRegexFilter";
  public static final String DEFAULT_SUBSETFILES_REGEX_FILTER = ".*";

  protected final Path rootPath;
  protected Pattern identifierPattern;
  @Getter
  @Setter
  protected PathFilter pathFilter;
  protected final Map<String, List<FileStatus>> idToFileStatuses;

  private Optional<EventSubmitter> eventSubmitter;
  private SourceState state;

  public SubsetFilesCopyableDatasetFinder(FileSystem fs, Properties props)
      throws IOException {
    super(fs, props);
    this.identifierPattern = Pattern.compile(props.getProperty(IDENTIFIER_PATTERN, DEFAULT_IDENTIFIER_PATTERN));
    this.pathFilter =
        new RegexPathFilter(props.getProperty(SUBSETFILES_REGEX_FILTER, DEFAULT_SUBSETFILES_REGEX_FILTER));
    this.rootPath = PathUtils.deepestNonGlobPath(this.datasetPattern);
    this.idToFileStatuses = new HashMap<>();
  }

  public SubsetFilesCopyableDatasetFinder(FileSystem fs, Properties props, EventSubmitter eventSubmitter)
      throws IOException {
    this(fs, props);
    this.eventSubmitter = Optional.of(eventSubmitter);
  }

  public SubsetFilesCopyableDatasetFinder(FileSystem fs, Properties props, EventSubmitter eventSubmitter,
      SourceState state)
      throws IOException {
    this(fs, props, eventSubmitter);
    this.state = state;
  }

  @Override
  public List<CopyableDataset> findDatasets()
      throws IOException {
    List<CopyableDataset> datasets = Lists.newArrayList();
    FileStatus[] fileStatuss = this.getDatasetDirs();
    for (FileStatus datasetRootDir : fileStatuss) {
      datasets.addAll(this.generateDatasetsByIdentifier(datasetRootDir.getPath()));
    }
    return datasets;
  }

  public List<CopyableDataset> generateDatasetsByIdentifier(Path datasetRootDirPath)
      throws IOException {
    List<CopyableDataset> datasets = Lists.newArrayList();
    FileStatus[] fileStatuses = fs.listStatus(datasetRootDirPath, this.getPathFilter());
    for (FileStatus fileStatus : fileStatuses) {
      Matcher result = this.identifierPattern.matcher(fileStatus.getPath().getName().toString());
      if (result.find()) {
        String id = result.group(1);
        if (idToFileStatuses.containsKey(id)) {
          log.debug("Adding " + fileStatus.getPath() + " to " + id);
          idToFileStatuses.get(id).add(fileStatus);
        } else {
          List<FileStatus> entry = new ArrayList<>();
          entry.add(fileStatus);
          log.debug("Adding " + fileStatus.getPath() + " to " + id);
          idToFileStatuses.put(id, entry);
        }
      }
    }
    for (String id : idToFileStatuses.keySet()) {
      datasets.add(this.datasetAndPathWithIdentifier(datasetRootDirPath, id));
    }
    return datasets;
  }

  public CopyableDataset datasetAndPathWithIdentifier(Path path, String identifier)
      throws IOException {
    try {
      return GobblinConstructorUtils
          .invokeLongestConstructor(SubsetFilesCopyableDataset.class, fs, path, props, identifier,
              idToFileStatuses.get(identifier), eventSubmitter, state);
    } catch (ReflectiveOperationException e) {
      throw new IOException(e);
    }
  }

  @Override
  public CopyableDataset datasetAtPath(Path path)
      throws IOException {
    throw new IOException("Not supported in " + this.getClass().getSimpleName());
  }
}

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

package org.apache.gobblin.temporal.ddm.work;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.temporal.ddm.work.styles.FileSystemApt;
import org.apache.gobblin.temporal.util.nesting.work.SeqSliceBackedWorkSpan;
import org.apache.gobblin.temporal.util.nesting.work.Workload;
import org.apache.gobblin.util.HadoopUtils;


/**
 * {@link Workload} of `WORK_ITEM`s (as defined by derived class) that originates from the eagerly loaded contents of
 * the directory `fsDir` within the {@link FileSystem} at `nameNodeUri`.
 *
 * IMPORTANT: to abide by Temporal's required determinism, a derived class must provide a {@link Comparator} for the
 * *total ordering* of `WORK_ITEM`s.
 */
@lombok.NoArgsConstructor // IMPORTANT: for jackson (de)serialization
@lombok.RequiredArgsConstructor
@lombok.ToString(exclude = { "cachedWorkItems" })
@Slf4j
public abstract class AbstractEagerFsDirBackedWorkload<WORK_ITEM> implements Workload<WORK_ITEM>, FileSystemApt {

  @Getter
  @NonNull private URI fileSystemUri;
  // NOTE: use `String` rather than `Path` to avoid: com.fasterxml.jackson.databind.exc.MismatchedInputException:
  //   Cannot construct instance of `org.apache.hadoop.fs.Path` (although at least one Creator exists):
  //     cannot deserialize from Object value (no delegate- or property-based Creator)
  @NonNull private String fsDir;
  private transient volatile WORK_ITEM[] cachedWorkItems = null;

  @Override
  public Optional<Workload.WorkSpan<WORK_ITEM>> getSpan(final int startIndex, final int numElements) {
    WORK_ITEM[] workItems = getCachedWorkItems();
    if (startIndex >= workItems.length || startIndex < 0) {
      return Optional.empty();
    } else {
      return Optional.of(new SeqSliceBackedWorkSpan<>(workItems, startIndex, numElements));
    }
  }

  @Override
  public boolean isIndexKnownToExceed(final int index) {
    return isDefiniteSize() && cachedWorkItems != null && index >= cachedWorkItems.length;
  }

  @Override
  @JsonIgnore // (because no-arg method resembles 'java bean property')
  public boolean isDefiniteSize() {
    return true;
  }

  protected abstract WORK_ITEM fromFileStatus(FileStatus fileStatus);

  /**
   *  IMPORTANT: to satisfy Temporal's required determinism, the `WORK_ITEM`s need a consistent total ordering
   *  WARNING: this works so long as dir contents are unchanged in iterim
   *  TODO: handle case of dir contents growing (e.g. use timestamp to filter out newer paths)... how could we handle the case of shrinking/deletion?
   */
  @JsonIgnore // (because no-arg method resembles 'java bean property')
  protected abstract Comparator<WORK_ITEM> getWorkItemComparator();

  /** Hook for each `WORK_ITEM` to be associated with its final, post-sorting ordinal index */
  protected void acknowledgeOrdering(int index, WORK_ITEM workItem) {
    // no-op
  }

  @JsonIgnore // (because no-arg method resembles 'java bean property')
  protected PathFilter getPathFilter() {
    return f -> true;
  }

  @JsonIgnore // (because no-arg method resembles 'java bean property')
  protected final synchronized WORK_ITEM[] getCachedWorkItems() {
    if (cachedWorkItems != null) {
      return cachedWorkItems;
    }
    try (FileSystem fs = loadFileSystem()) {
      FileStatus[] fileStatuses = fs.listStatus(new Path(fsDir), this.getPathFilter());
      log.info("loaded {} paths from '{}'", fileStatuses.length, fsDir);
      WORK_ITEM[] workItems = (WORK_ITEM[])Stream.of(fileStatuses).map(this::fromFileStatus).toArray(Object[]::new);
      sortWorkItems(workItems);
      IntStream.range(0, workItems.length)
          .forEach(i -> this.acknowledgeOrdering(i, workItems[i]));
      cachedWorkItems = workItems;
      return cachedWorkItems;
    } catch (FileNotFoundException fnfe) {
      throw new RuntimeException("directory not found: '" + fsDir + "'");
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @JsonIgnore // (because no-arg method resembles 'java bean property')
  @Override
  public State getFileSystemConfig() {
    return new State(); // TODO - figure out how to truly set!
  }

  @JsonIgnore // (because no-arg method resembles 'java bean property')
  protected FileSystem loadFileSystem() throws IOException {
    return HadoopUtils.getFileSystem(this.fileSystemUri, this.getFileSystemConfig());
  }

  private void sortWorkItems(WORK_ITEM[] workItems) {
    Arrays.sort(workItems, getWorkItemComparator());
  }
}

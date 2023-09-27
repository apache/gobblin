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

package org.apache.gobblin.temporal.ddm.activity.impl;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.temporal.ddm.activity.ProcessWorkUnit;
import org.apache.gobblin.temporal.ddm.work.WorkUnitClaimCheck;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.JobLauncherUtils;
import org.apache.gobblin.util.SerializationUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


@Slf4j
public class ProcessWorkUnitImpl implements ProcessWorkUnit {
  // TODO: replace w/ JobLauncherUtils (once committed)!!!
  public static final String MULTI_WORK_UNIT_FILE_EXTENSION = ".mwu";
  public static final int FS_CACHE_TTL_SECS = 5 * 60;

  private final State stateConfig;
  // cache `FileSystem`s to avoid re-opening what a recent prior execution already has
  private final transient LoadingCache<URI, FileSystem> fsByUri = CacheBuilder.newBuilder()
      .expireAfterWrite(FS_CACHE_TTL_SECS, TimeUnit.SECONDS)
      .removalListener((RemovalNotification<URI, org.apache.hadoop.fs.FileSystem> notification) -> {
        try {
          notification.getValue().close(); // prevent resource leak from cache eviction
        } catch (IOException ioe) {
          log.warn("trouble closing (cache-evicted) `hadoop.fs.FileSystem` at '" + notification.getKey() + "'", ioe);
          // otherwise swallow, since within a removal listener thread
        }
      })
      .build(new CacheLoader<URI, FileSystem>() {
    @Override
    public FileSystem load(URI fsUri) throws IOException {
      return ProcessWorkUnitImpl.this.loadFileSystemForUri(fsUri);
    }
  });

  public ProcessWorkUnitImpl(Optional<State> optStateConfig) {
    this.stateConfig = optStateConfig.orElseGet(State::new);
  }

  public ProcessWorkUnitImpl() {
    this(Optional.empty());
  }

  @Override
  public int processWorkUnit(WorkUnitClaimCheck wu) {
    try {
      List<WorkUnit> workUnits = loadFlattenedWorkUnits(wu);
      // TODO - fill in actual execution (counting the properties is merely to validate/debug integration)
      int totalNumProps = workUnits.stream().mapToInt(workUnit -> workUnit.getPropertyNames().size()).sum();
      log.info("opened WU [{}] to find {} properties total at '{}'", wu.getCorrelator(), totalNumProps, wu.getWorkUnitPath());
      return totalNumProps;
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  protected List<WorkUnit> loadFlattenedWorkUnits(WorkUnitClaimCheck wu) throws IOException {
    FileSystem fs = this.loadFileSystem(wu);
    Path wuPath = new Path(wu.getWorkUnitPath());
    WorkUnit workUnit = (wuPath.toString().endsWith(MULTI_WORK_UNIT_FILE_EXTENSION) ? MultiWorkUnit.createEmpty()
        : WorkUnit.createEmpty());
    SerializationUtils.deserializeState(fs, wuPath, workUnit);

    return workUnit instanceof MultiWorkUnit
        ? JobLauncherUtils.flattenWorkUnits(((MultiWorkUnit) workUnit).getWorkUnits())
        : Lists.newArrayList(workUnit);
  }

  protected final FileSystem loadFileSystem(WorkUnitClaimCheck wu) throws IOException {
    try {
      return fsByUri.get(wu.getNameNodeUri());
    } catch (ExecutionException ee) {
      throw new IOException(ee);
    }
  }

  protected FileSystem loadFileSystemForUri(URI fsUri) throws IOException {
    return HadoopUtils.getFileSystem(fsUri, stateConfig);
  }
}

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

package gobblin.data.management.copy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.fs.FileSystem;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.typesafe.config.Config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.data.management.copy.watermark.CopyableFileWatermarkHelper;
import gobblin.data.management.copy.watermark.CopyableFileWatermarkGenerator;
import gobblin.source.extractor.ComparableWatermark;
import gobblin.source.extractor.Watermark;
import gobblin.source.extractor.WatermarkInterval;
import gobblin.source.extractor.WatermarkSerializerHelper;
import gobblin.util.ConfigUtils;


/**
 * Implementation of {@link CopyableFileFilter} that filters {@link CopyableFile}s based on previous watermark information.
 */
@Slf4j
public class WatermarkBasedCopyableFileFilter implements CopyableFileFilter {
  private static final int DEFAULT_MAX_CACHE_SIZE = 10;

  protected final SourceState sourcestate;
  protected final CopyableDatasetMetadata copyableDatasetMetadata;
  protected final Optional<CopyableFileWatermarkGenerator> copyableFileWatermarkGeneratorOptional;
  private final Callable<Map<String, IncludeExcludeWatermark>> callablePrevWatermarkMap;
  private final FILTER_POLICY filterPolicy;

  public static final String WATERMARK_FILTER_POLICY = "WatermarkBasedCopyableFileFilter.filterPolicy";

  /**
   * Policy for filtering based on watermarks from previous workunitstates:
   * 1. UNCOMMITTED_FIRST: Uncommitted workunitstates will be considered first.
   *    The lowest value among all uncommitted workunit states will be used as watermark.
   * 2. COMMITTED_FIRST: Committed workunitstates will be considered first.
   *    The highest value among all committed workunit states will be used as watermark.
   */
  public enum FILTER_POLICY {
    UNCOMMITTED_FIRST,
    COMMITTED_FIRST
  }

  private static final Cache<SourceState, Map<String, IncludeExcludeWatermark>> STATE_TO_PREV_WATERMARK_MAP_CAHCE =
      CacheBuilder.newBuilder().maximumSize(DEFAULT_MAX_CACHE_SIZE).build();

  public WatermarkBasedCopyableFileFilter(SourceState state, CopyableDataset copyableDataset)
      throws IOException {
    Preconditions.checkNotNull(state);
    this.sourcestate = state;
    this.copyableFileWatermarkGeneratorOptional = CopyableFileWatermarkHelper.getCopyableFileWatermarkGenerator(state);
    Preconditions.checkArgument(this.copyableFileWatermarkGeneratorOptional.isPresent());
    this.copyableDatasetMetadata = new CopyableDatasetMetadata(copyableDataset);
    this.callablePrevWatermarkMap = new Callable<Map<String, IncludeExcludeWatermark>>() {
      @Override
      public Map<String, IncludeExcludeWatermark> call()
          throws Exception {
        return getPreviousWatermarkForFilter(sourcestate, copyableFileWatermarkGeneratorOptional);
      }
    };
    Config conf = ConfigUtils.propertiesToConfig(state.getProperties());
    this.filterPolicy =
        conf.hasPath(WATERMARK_FILTER_POLICY) ? FILTER_POLICY.valueOf(conf.getString(WATERMARK_FILTER_POLICY))
            : FILTER_POLICY.UNCOMMITTED_FIRST;
  }

  @Override
  public Collection<CopyableFile> filter(FileSystem sourceFs, FileSystem targetFs,
      Collection<CopyableFile> copyableFiles) {
    List<CopyableFile> filteredCopyableFiles = new ArrayList<>();
    try {
      for (CopyableFile copyableFile : copyableFiles) {
        Optional<WatermarkInterval> watermarkIntervalOptional = CopyableFileWatermarkHelper
            .getCopyableFileWatermark(copyableFile, this.copyableFileWatermarkGeneratorOptional);
        Map<String, IncludeExcludeWatermark> prevWatermarkMap =
            STATE_TO_PREV_WATERMARK_MAP_CAHCE.get(this.sourcestate, this.callablePrevWatermarkMap);
        String datasetURN = copyableFile.getDatasetAndPartition(this.copyableDatasetMetadata).toString();
        if (!shouldSkipDueToWatermark(prevWatermarkMap.get(datasetURN), watermarkIntervalOptional)) {
          log.info("Adding one copyableFile after checking watermark: " + copyableFile);
          filteredCopyableFiles.add(copyableFile);
        }
      }
      return filteredCopyableFiles;
    } catch (IOException | ExecutionException e) {
      throw new RuntimeException("Failed to filter copyableFiles.", e);
    }
  }

  protected boolean shouldSkipDueToWatermark(IncludeExcludeWatermark previousWatermark,
      Optional<WatermarkInterval> curWatermarkIntervalOptional) {
    if (curWatermarkIntervalOptional.isPresent() && previousWatermark != null) {
      if (previousWatermark.isInclude()) {
        return ((ComparableWatermark) (curWatermarkIntervalOptional.get().getExpectedHighWatermark()))
            .compareTo(previousWatermark.getWatermark()) < 0;
      } else {
        return ((ComparableWatermark) (curWatermarkIntervalOptional.get().getExpectedHighWatermark()))
            .compareTo(previousWatermark.getWatermark()) <= 0;
      }
    } else {
      return false;
    }
  }

  /**
   * Return the mapping from datasetUrns to {@link IncludeExcludeWatermark}s based on previous workunitstates.
   */
  protected Map<String, IncludeExcludeWatermark> getPreviousWatermarkForFilter(SourceState state,
      Optional<CopyableFileWatermarkGenerator> watermarkGenerator)
      throws IOException {
    Map<String, Iterable<WorkUnitState>> previousWorkUnitStatesByDatasetUrns =
        state.getPreviousWorkUnitStatesByDatasetUrns();
    Map<String, IncludeExcludeWatermark> previousActualWatermarkByDatasetUrns = new HashMap<>();
    for (Map.Entry<String, Iterable<WorkUnitState>> workUnitStatesPerDatasetURN : previousWorkUnitStatesByDatasetUrns
        .entrySet()) {
      IncludeExcludeWatermark previousWatermark =
          this.getPreviousWatermarkPerDataset(workUnitStatesPerDatasetURN.getValue(), watermarkGenerator);
      if (previousWatermark != null) {
        previousActualWatermarkByDatasetUrns.put(workUnitStatesPerDatasetURN.getKey(), previousWatermark);
      }
    }
    return previousActualWatermarkByDatasetUrns;
  }

  /**
   * Return {@link IncludeExcludeWatermark} for each dataset's workunitstates, based on {@link #filterPolicy}.
   */
  protected IncludeExcludeWatermark getPreviousWatermarkPerDataset(Iterable<WorkUnitState> workUnitStates,
      Optional<CopyableFileWatermarkGenerator> watermarkGenerator) {
    List<ComparableWatermark> watermarksOfCommittedWus = new ArrayList<>();
    List<ComparableWatermark> watermarksOfUncommittedWus = new ArrayList<>();
    Class<? extends ComparableWatermark> watermarkClass = watermarkGenerator.get().getWatermarkClass();

    for (WorkUnitState workUnitState : workUnitStates) {
      if (workUnitState.getWorkingState().equals(WorkUnitState.WorkingState.COMMITTED)) {
        Watermark curWatermark = WatermarkSerializerHelper
            .convertJsonToWatermark(workUnitState.getWorkunit().getExpectedHighWatermark(), watermarkClass);
        if (curWatermark != null) {
          watermarksOfCommittedWus.add((ComparableWatermark) curWatermark);
        }
      } else {
        Watermark curWatermark = WatermarkSerializerHelper
            .convertJsonToWatermark(workUnitState.getWorkunit().getLowWatermark(), watermarkClass);
        if (curWatermark != null) {
          watermarksOfUncommittedWus.add((ComparableWatermark) curWatermark);
        }
      }
    }
    Collections.sort(watermarksOfCommittedWus);
    Collections.sort(watermarksOfUncommittedWus);

    if (this.filterPolicy == FILTER_POLICY.UNCOMMITTED_FIRST) {
      if (!watermarksOfUncommittedWus.isEmpty()) {
        return new IncludeExcludeWatermark(watermarksOfUncommittedWus.get(0), true);
      } else if (!watermarksOfCommittedWus.isEmpty()) {
        return new IncludeExcludeWatermark(watermarksOfCommittedWus.get(watermarksOfCommittedWus.size() - 1), false);
      }
    } else {
      if (!watermarksOfCommittedWus.isEmpty()) {
        return new IncludeExcludeWatermark(watermarksOfCommittedWus.get(watermarksOfCommittedWus.size() - 1), false);
      } else if (!watermarksOfUncommittedWus.isEmpty()) {
        return new IncludeExcludeWatermark(watermarksOfUncommittedWus.get(0), true);
      }
    }
    return null;
  }


  /**
   * Class for watermark with an indicator {@link IncludeExcludeWatermark#include}.
   * True means the watermark has not been committed, and the watermark should be included for next run.
   */
  @Data
  @AllArgsConstructor
  @VisibleForTesting
  protected static class IncludeExcludeWatermark {
    ComparableWatermark watermark;
    boolean include;
  }
}

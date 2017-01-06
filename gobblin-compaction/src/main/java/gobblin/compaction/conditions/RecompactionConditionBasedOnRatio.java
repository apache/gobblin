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

package gobblin.compaction.conditions;


import java.util.List;
import java.util.Map;

import gobblin.annotation.Alias;
import gobblin.compaction.dataset.DatasetHelper;
import gobblin.compaction.dataset.Dataset;
import gobblin.compaction.mapreduce.MRCompactor;
import gobblin.util.DatasetFilterUtils;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;


/**
 * An implementation {@link RecompactionCondition} which examines the late record percentage.
 * If the percent exceeds the limit, a recompaction is triggered.
 */
@Alias("RecompactionConditionBasedOnRatio")
public class RecompactionConditionBasedOnRatio implements RecompactionCondition {
  public static final char DATASETS_WITH_DIFFERENT_RECOMPACT_THRESHOLDS_SEPARATOR = ';';
  public static final char DATASETS_WITH_SAME_RECOMPACT_THRESHOLDS_SEPARATOR = ',';
  public static final char DATASETS_AND_RECOMPACT_THRESHOLD_SEPARATOR = ':';

  private static final Logger logger = LoggerFactory.getLogger (RecompactionConditionBasedOnRatio.class);
  private final double ratio;

  private RecompactionConditionBasedOnRatio (Dataset dataset) {
    Map<String, Double> datasetRegexAndRecompactThreshold = getDatasetRegexAndRecompactThreshold(
        dataset.jobProps().getProp(
            MRCompactor.COMPACTION_LATEDATA_THRESHOLD_FOR_RECOMPACT_PER_DATASET, StringUtils.EMPTY));
    this.ratio = getOwnRatioThreshold (dataset, datasetRegexAndRecompactThreshold);
  }

  @Alias("RecompactBasedOnRatio")
  public static class Factory implements RecompactionConditionFactory {
    @Override public RecompactionCondition createRecompactionCondition (Dataset dataset) {
      return new RecompactionConditionBasedOnRatio (dataset);
    }
  }

  private static Map<String, Double> getDatasetRegexAndRecompactThreshold (String datasetsAndRecompactThresholds) {
    Map<String, Double> topicRegexAndRecompactThreshold = Maps.newHashMap();
    for (String entry : Splitter.on(DATASETS_WITH_DIFFERENT_RECOMPACT_THRESHOLDS_SEPARATOR).trimResults()
        .omitEmptyStrings().splitToList(datasetsAndRecompactThresholds)) {
      List<String> topicsAndRecompactThreshold =
          Splitter.on(DATASETS_AND_RECOMPACT_THRESHOLD_SEPARATOR).trimResults().omitEmptyStrings().splitToList(entry);
      if (topicsAndRecompactThreshold.size() != 2) {
        logger.error("Invalid form (DATASET_NAME:THRESHOLD) in "
            + MRCompactor.COMPACTION_LATEDATA_THRESHOLD_FOR_RECOMPACT_PER_DATASET + ".");
      } else {
        topicRegexAndRecompactThreshold.put(topicsAndRecompactThreshold.get(0),
            Double.parseDouble(topicsAndRecompactThreshold.get(1)));
      }
    }
    return topicRegexAndRecompactThreshold;
  }

  private double getOwnRatioThreshold (Dataset dataset, Map<String, Double> datasetRegexAndRecompactThreshold) {
    for (Map.Entry<String, Double> topicRegexEntry : datasetRegexAndRecompactThreshold.entrySet()) {
      if (DatasetFilterUtils.stringInPatterns(dataset.getDatasetName(),
          DatasetFilterUtils.getPatternsFromStrings(Splitter.on(DATASETS_WITH_SAME_RECOMPACT_THRESHOLDS_SEPARATOR)
              .trimResults().omitEmptyStrings().splitToList(topicRegexEntry.getKey())))) {
        return topicRegexEntry.getValue();
      }
    }
    return MRCompactor.DEFAULT_COMPACTION_LATEDATA_THRESHOLD_FOR_RECOMPACT_PER_DATASET;
  }

  public boolean isRecompactionNeeded (DatasetHelper datasetHelper) {

    long lateDataCount = datasetHelper.getLateOutputRecordCount();
    long nonLateDataCount = datasetHelper.getOutputRecordCount();
    double lateDataPercent = lateDataCount * 1.0 /  (lateDataCount + nonLateDataCount);
    logger.info ("Late data ratio is " + lateDataPercent + " and threshold is " + this.ratio);
    if (lateDataPercent > ratio) {
      return true;
    }

    return false;
  }
}
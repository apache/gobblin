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

package gobblin.source.extractor.extract.kafka.workunit.packer;

import java.util.List;
import java.util.Map;

import org.apache.commons.math3.stat.descriptive.moment.GeometricMean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.extract.kafka.KafkaPartition;
import gobblin.source.extractor.extract.kafka.KafkaUtils;
import gobblin.source.workunit.WorkUnit;


/**
 * An implementation of {@link KafkaWorkUnitSizeEstimator} which uses the average time to pull a record in the
 * previous run to estimate the sizes of {@link WorkUnits}.
 *
 * Each partition pulled in the previous run should have an avg time per record in its {@link WorkUnitState}. In the
 * next run, the estimated avg time per record for each topic is the geometric mean of the avg time per record of all
 * partitions. For example if a topic has two partitions whose avg time per record in the previous run are 2 and 8,
 * the next run will use 4 as the estimated avg time per record. The reason to choose geometric mean over algebraic
 * mean is because large numbers are likely outliers, e.g., a topic may have 5 partitions, and the avg time per record
 * collected from the previous run could sometimes be [1.1, 1.2, 1.1, 1.3, 100].
 *
 * If a topic was not pulled in the previous run, its estimated avg time per record is the geometric mean of the
 * estimated avg time per record of all topics that were pulled in the previous run. If no topic was pulled in the
 * previous run, a default value of 1.0 is used.
 *
 * @author Ziyang Liu
 */
public class KafkaAvgRecordTimeBasedWorkUnitSizeEstimator implements KafkaWorkUnitSizeEstimator {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaAvgRecordTimeBasedWorkUnitSizeEstimator.class);

  private static final GeometricMean GEOMETRIC_MEAN = new GeometricMean();
  private static final double EPS = 0.01;

  private final Map<String, Double> estAvgMillis = Maps.newHashMap();
  private double avgEstAvgMillis = 0.0;

  KafkaAvgRecordTimeBasedWorkUnitSizeEstimator(SourceState state) {
    readPrevAvgRecordMillis(state);
  }

  @Override
  public double calcEstimatedSize(WorkUnit workUnit) {
    double avgMillis = this.getEstAvgMillisForTopic(KafkaUtils.getTopicName(workUnit));
    long numOfRecords = workUnit.getPropAsLong(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY)
        - workUnit.getPropAsLong(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY);
    return avgMillis * numOfRecords;
  }

  /**
   * Calculate the geometric mean of a {@link List} of double numbers. Numbers smaller than {@link #EPS} will be
   * treated as {@link #EPS}.
   */
  private static double geometricMean(List<Double> numbers) {
    Preconditions.checkArgument(!numbers.isEmpty());

    double[] numberArray = new double[numbers.size()];
    for (int i = 0; i < numbers.size(); i++) {
      numberArray[i] = Math.max(numbers.get(i), EPS);
    }

    return GEOMETRIC_MEAN.evaluate(numberArray, 0, numberArray.length);
  }

  private double getEstAvgMillisForTopic(String topic) {
    if (this.estAvgMillis.containsKey(topic)) {
      return this.estAvgMillis.get(topic);
    }
    return this.avgEstAvgMillis;
  }

  /**
   * Get avg time to pull a record in the previous run for all topics, each of which is the geometric mean
   * of the avg time to pull a record of all partitions of the topic.
   *
   * If a topic was not pulled in the previous run (e.g., it's a new topic), it will use the geometric mean
   * of avg record time of topics that were pulled in the previous run.
   *
   * If no topic was pulled in the previous run, 1.0 will be used for all topics.
   */
  private void readPrevAvgRecordMillis(SourceState state) {
    Map<String, List<Double>> prevAvgMillis = Maps.newHashMap();

    for (WorkUnitState workUnitState : state.getPreviousWorkUnitStates()) {
      List<KafkaPartition> partitions = KafkaUtils.getPartitions(workUnitState);
      for (KafkaPartition partition : partitions) {
        if (KafkaUtils.containsPartitionAvgRecordMillis(workUnitState, partition)) {
          double prevAvgMillisForPartition = KafkaUtils.getPartitionAvgRecordMillis(workUnitState, partition);
          if (prevAvgMillis.containsKey(partition.getTopicName())) {
            prevAvgMillis.get(partition.getTopicName()).add(prevAvgMillisForPartition);
          } else {
            prevAvgMillis.put(partition.getTopicName(), Lists.newArrayList(prevAvgMillisForPartition));
          }
        }
      }
    }
    this.estAvgMillis.clear();
    if (prevAvgMillis.isEmpty()) {
      this.avgEstAvgMillis = 1.0;
    } else {
      List<Double> allEstAvgMillis = Lists.newArrayList();
      for (Map.Entry<String, List<Double>> entry : prevAvgMillis.entrySet()) {
        String topic = entry.getKey();
        List<Double> prevAvgMillisForPartitions = entry.getValue();

        // If a topic has k partitions, and in the previous run, each partition recorded its avg time to pull
        // a record, then use the geometric mean of these k numbers as the estimated avg time to pull
        // a record in this run.
        double estAvgMillisForTopic = geometricMean(prevAvgMillisForPartitions);
        this.estAvgMillis.put(topic, estAvgMillisForTopic);
        LOG.info(String.format("Estimated avg time to pull a record for topic %s is %f milliseconds", topic,
            estAvgMillisForTopic));
        allEstAvgMillis.add(estAvgMillisForTopic);
      }

      // If a topic was not pulled in the previous run, use this.avgEstAvgMillis as the estimated avg time
      // to pull a record in this run, which is the geometric mean of all topics whose avg times to pull
      // a record in the previous run are known.
      this.avgEstAvgMillis = geometricMean(allEstAvgMillis);
    }
    LOG.info("For all topics not pulled in the previous run, estimated avg time to pull a record is "
        + this.avgEstAvgMillis + " milliseconds");
  }
}

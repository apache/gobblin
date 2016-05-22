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

import com.google.common.collect.Lists;
import com.google.common.math.DoubleMath;

import gobblin.configuration.SourceState;
import gobblin.source.extractor.extract.AbstractSource;
import gobblin.source.workunit.MultiWorkUnit;
import gobblin.source.workunit.WorkUnit;


/**
 * An implementation of {@link KafkaWorkUnitPacker} with a single level of bin packing using worst-fit-decreasing.
 *
 * Note that for each skipped topic, an empty workunit is created for each partition to preserve its checkpoints.
 * In single-level bin packing mode, it still assigns all partitions of a skipped topic to the same workunit / task,
 * so that a single empty task will be created for this topic, instead of one empty task per partition.
 *
 * Please refer to the Javadoc of {@link KafkaBiLevelWorkUnitPacker} for a comparison between
 * {@link KafkaSingleLevelWorkUnitPacker} and {@link KafkaBiLevelWorkUnitPacker}.
 *
 * @author Ziyang Liu
 */
public class KafkaSingleLevelWorkUnitPacker extends KafkaWorkUnitPacker {

  protected KafkaSingleLevelWorkUnitPacker(AbstractSource<?, ?> source, SourceState state) {
    super(source, state);
  }

  @Override
  public List<WorkUnit> pack(Map<String, List<WorkUnit>> workUnitsByTopic, int numContainers) {
    setWorkUnitEstSizes(workUnitsByTopic);
    List<WorkUnit> workUnits = Lists.newArrayList();
    for (List<WorkUnit> workUnitsForTopic : workUnitsByTopic.values()) {

      // For each topic, merge all empty workunits into a single workunit, so that a single
      // empty task will be created instead of many.
      MultiWorkUnit zeroSizeWorkUnit = MultiWorkUnit.createEmpty();
      for (WorkUnit workUnit : workUnitsForTopic) {
        if (DoubleMath.fuzzyEquals(getWorkUnitEstSize(workUnit), 0.0, EPS)) {
          addWorkUnitToMultiWorkUnit(workUnit, zeroSizeWorkUnit);
        } else {
          workUnit.setWatermarkInterval(getWatermarkIntervalFromWorkUnit(workUnit));
          workUnits.add(workUnit);
        }
      }
      if (!zeroSizeWorkUnit.getWorkUnits().isEmpty()) {
        workUnits.add(squeezeMultiWorkUnit(zeroSizeWorkUnit));
      }
    }
    return worstFitDecreasingBinPacking(workUnits, numContainers);
  }
}

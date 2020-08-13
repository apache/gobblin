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

package org.apache.gobblin.source.extractor.extract.kafka.workunit.packer;

import java.util.List;
import java.util.Map;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.source.extractor.extract.AbstractSource;
import org.apache.gobblin.source.workunit.WorkUnit;

import static org.apache.gobblin.source.extractor.extract.kafka.workunit.packer.KafkaWorkUnitPacker.KAFKA_WORKUNIT_PACKER_CUSTOMIZED_TYPE;
import static org.apache.gobblin.source.extractor.extract.kafka.workunit.packer.KafkaWorkUnitPacker.KAFKA_WORKUNIT_PACKER_TYPE;
import static org.apache.gobblin.source.extractor.extract.kafka.workunit.packer.KafkaWorkUnitPacker.KAFKA_WORKUNIT_SIZE_ESTIMATOR_CUSTOMIZED_TYPE;
import static org.apache.gobblin.source.extractor.extract.kafka.workunit.packer.KafkaWorkUnitPacker.KAFKA_WORKUNIT_SIZE_ESTIMATOR_TYPE;


public class KafkaWorkUnitPackerTest {
  private KafkaWorkUnitPacker packer;
  AbstractSource source = Mockito.mock(AbstractSource.class);
  SourceState state;

  @BeforeMethod
  public void setUp() {
    state = new SourceState();

    // Using customized type and having customized as a known class.
    state.setProp(KAFKA_WORKUNIT_PACKER_TYPE, "CUSTOM");
    state.setProp(KAFKA_WORKUNIT_PACKER_CUSTOMIZED_TYPE,
        "org.apache.gobblin.source.extractor.extract.kafka.workunit.packer.KafkaSingleLevelWorkUnitPacker");
    state.setProp(KAFKA_WORKUNIT_SIZE_ESTIMATOR_TYPE, "CUSTOM");
    state.setProp(KAFKA_WORKUNIT_SIZE_ESTIMATOR_CUSTOMIZED_TYPE,
        "org.apache.gobblin.source.extractor.extract.kafka.workunit.packer.KafkaAvgRecordTimeBasedWorkUnitSizeEstimator");
    packer = new TestKafkaWorkUnitPacker(source, state);
  }

  @Test
  public void testGetWorkUnitSizeEstimator() {
    KafkaWorkUnitSizeEstimator estimator = packer.getWorkUnitSizeEstimator();
    Assert.assertTrue(estimator instanceof KafkaAvgRecordTimeBasedWorkUnitSizeEstimator);
  }

  @Test
  public void testGetInstance() {
    KafkaWorkUnitPacker anotherPacker = KafkaWorkUnitPacker.getInstance(source, state);
    Assert.assertTrue(anotherPacker instanceof KafkaSingleLevelWorkUnitPacker);
  }

  @Test
  public void testPackEmptyWorkUnit() {
    SourceState sourceState = new SourceState(state);
    Map<String, List<WorkUnit>> emptyWorkUnit = Maps.newHashMap();
    // Test single level packer
    KafkaWorkUnitPacker mypacker = KafkaWorkUnitPacker.getInstance(source, sourceState);
    Assert.assertEquals(mypacker.pack(emptyWorkUnit, 1).size(), 0);
    // Test bi level packer
    sourceState.setProp(KAFKA_WORKUNIT_PACKER_CUSTOMIZED_TYPE,
        "org.apache.gobblin.source.extractor.extract.kafka.workunit.packer.KafkaBiLevelWorkUnitPacker");
    mypacker = KafkaWorkUnitPacker.getInstance(source, sourceState);
    Assert.assertEquals(mypacker.pack(emptyWorkUnit, 1).size(), 0);
  }

  public class TestKafkaWorkUnitPacker extends KafkaWorkUnitPacker {
    public TestKafkaWorkUnitPacker(AbstractSource<?, ?> source, SourceState state) {
      super(source, state);
    }

    // Dummy implementation for making abstract class instantiable only.
    @Override
    public List<WorkUnit> pack(Map<String, List<WorkUnit>> workUnitsByTopic, int numContainers) {
      return null;
    }
  }
}
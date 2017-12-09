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

package org.apache.gobblin.converter;

import java.util.Iterator;
import java.util.Properties;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.WorkUnitState;


@Test
public class SamplingConverterTest {

  private void assertNear(int actual, int expected, int threshold, String context) {
    boolean near = (Math.abs(actual - expected) <= Math.abs(threshold));
    Assert.assertTrue(near, context + ": Failed nearness test between "
        + actual + " and " + expected + " with threshold " + threshold);
  }


  public void testSampling()
      throws DataConversionException {

    int numIterations = 100;
    Random random = new Random();
    for (int j = 0; j < numIterations; ++j) {

      SamplingConverter sampler = new SamplingConverter();
      Properties props = new Properties();
      float randomSampling = random.nextFloat();
      props.setProperty(SamplingConverter.SAMPLE_RATIO_KEY, "" + randomSampling);
      WorkUnitState workUnitState = new WorkUnitState();
      workUnitState.addAll(props);

      sampler.init(workUnitState);

      int numRecords = 10000; // need at least 10k samples
      int sampledRecords = 0;
      for (int i = 0; i < numRecords; ++i) {
        Object o = new Object();
        Iterator<Object> recordIter = sampler.convertRecord(null, o, workUnitState).iterator();
        if (recordIter.hasNext()) {
          ++sampledRecords;
          // make sure we got back the same record
          Assert.assertEquals(recordIter.next(), o, "Sampler should return the same record");
          Assert.assertFalse(recordIter.hasNext(), "There should only be 1 record returned");
        }
      }
      int threshold = (int) (0.02 * (double) numRecords); // 2 %
      assertNear(sampledRecords, (int) (randomSampling * (float) numRecords), threshold, "SampleRatio: " + randomSampling);
    }
  }
}

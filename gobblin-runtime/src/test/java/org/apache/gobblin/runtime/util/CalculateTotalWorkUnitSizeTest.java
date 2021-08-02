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

package org.apache.gobblin.runtime.util;


import java.util.ArrayList;
import java.util.List;
import org.apache.gobblin.runtime.AbstractJobLauncher;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.source.workunit.BasicWorkUnitStream;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.source.workunit.WorkUnitStream;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CalculateTotalWorkUnitSizeTest {

  @Test
  public void testSummingWorkUnitsIncreasingSize() throws Exception {
    List<WorkUnit> workunits = new ArrayList();
    WorkUnit newWorkUnit;
    for (int i=1; i < 11; i++) {
      newWorkUnit = new WorkUnit();
      newWorkUnit.setProp(ServiceConfigKeys.WORK_UNIT_SIZE, i);
      workunits.add(newWorkUnit);
    }

    WorkUnitStream workUnitStream = new BasicWorkUnitStream.Builder(workunits).build();
    long totalBytesToCopy = AbstractJobLauncher.sumWorkUnitsSizes(workUnitStream);

    Assert.assertEquals(totalBytesToCopy, 55);
  }

  @Test
  public void testSummingWorkUnitsArithmeticSum() throws Exception {
    List<WorkUnit> workunits = new ArrayList();
    WorkUnit newWorkUnit;
    for (int i=0; i < 10; i++) {
      newWorkUnit = new WorkUnit();
      newWorkUnit.setProp(ServiceConfigKeys.WORK_UNIT_SIZE, 3+5*i);
      workunits.add(newWorkUnit);
    }

    WorkUnitStream workUnitStream = new BasicWorkUnitStream.Builder(workunits).build();
    long totalBytesToCopy = AbstractJobLauncher.sumWorkUnitsSizes(workUnitStream);

    Assert.assertEquals(totalBytesToCopy, 255);
  }
}

/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.configuration.workunit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.io.Closer;

import gobblin.source.workunit.MultiWorkUnit;
import gobblin.source.workunit.WorkUnit;


/**
 * Unit tests for {@link MultiWorkUnit}.
 */
@Test(groups = {"gobblin.configuration.workunit"})
public class MultiWorkUnitTest {

  private MultiWorkUnit multiWorkUnit;

  @BeforeClass
  public void setUp() {
    this.multiWorkUnit = new MultiWorkUnit();

    WorkUnit workUnit1 = new WorkUnit();
    workUnit1.setHighWaterMark(1000);
    workUnit1.setLowWaterMark(0);
    workUnit1.setProp("k1", "v1");
    this.multiWorkUnit.addWorkUnit(workUnit1);

    WorkUnit workUnit2 = new WorkUnit();
    workUnit2.setHighWaterMark(2000);
    workUnit2.setLowWaterMark(1001);
    workUnit2.setProp("k2", "v2");
    this.multiWorkUnit.addWorkUnit(workUnit2);
  }

  @Test
  public void testSerDe()
      throws IOException {
    Closer closer = Closer.create();
    try {
      ByteArrayOutputStream baos = closer.register(new ByteArrayOutputStream());
      DataOutputStream dos = closer.register(new DataOutputStream(baos));
      this.multiWorkUnit.write(dos);

      ByteArrayInputStream bais = closer.register((new ByteArrayInputStream(baos.toByteArray())));
      DataInputStream dis = closer.register((new DataInputStream(bais)));
      MultiWorkUnit copy = new MultiWorkUnit();
      copy.readFields(dis);

      List<WorkUnit> workUnitList = copy.getWorkUnits();
      Assert.assertEquals(workUnitList.size(), 2);

      Assert.assertEquals(workUnitList.get(0).getHighWaterMark(), 1000);
      Assert.assertEquals(workUnitList.get(0).getLowWaterMark(), 0);
      Assert.assertEquals(workUnitList.get(0).getProp("k1"), "v1");

      Assert.assertEquals(workUnitList.get(1).getHighWaterMark(), 2000);
      Assert.assertEquals(workUnitList.get(1).getLowWaterMark(), 1001);
      Assert.assertEquals(workUnitList.get(1).getProp("k2"), "v2");
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }
}

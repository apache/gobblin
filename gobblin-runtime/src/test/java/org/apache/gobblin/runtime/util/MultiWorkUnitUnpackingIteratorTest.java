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

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;
import java.util.List;
import javax.annotation.Nullable;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MultiWorkUnitUnpackingIteratorTest {

  public static final String WORK_UNIT_NAME = "work.unit.name";

  @Test
  public void test() {

    List<WorkUnit> workUnitList = Lists.newArrayList();
    workUnitList.add(createWorkUnit("wu1", "wu2"));
    workUnitList.add(createWorkUnit("wu3"));
    workUnitList.add(createWorkUnit("wu4", "wu5", "wu6"));

    List<String> names = Lists.newArrayList(Iterators.transform(new MultiWorkUnitUnpackingIterator(workUnitList.iterator()),
        new Function<WorkUnit, String>() {
      @Nullable
      @Override
      public String apply(@Nullable WorkUnit input) {
        return input.getProp(WORK_UNIT_NAME);
      }
    }));

    Assert.assertEquals(names.size(), 6);
    for (int i = 0; i < 6; i++) {
      Assert.assertEquals(names.get(i), "wu" + (i + 1));
    }
  }

  private WorkUnit createWorkUnit(String... names) {
    if (names.length == 1) {
      WorkUnit workUnit = new WorkUnit();
      workUnit.setProp(WORK_UNIT_NAME, names[0]);
      return workUnit;
    }
    MultiWorkUnit mwu = new MultiWorkUnit();
    for (String name : names) {
      mwu.addWorkUnit(createWorkUnit(name));
    }
    return mwu;
  }

}

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

package org.apache.gobblin.source.workunit;

import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;


@Test(groups = { "gobblin.source.workunit" })
public class ExtractFactoryTest {

  /**
   * Verify that each {@link Extract} created by an {@ExtractFactory} has a unique ID.
   */
  @Test
  public void testGetUniqueExtract() {
    ExtractFactory extractFactory = new ExtractFactory("yyyyMMddHHmmss");
    Set<String> extractIDs = Sets.newHashSet();
    int numOfExtracts = 100;
    for (int i = 0; i < numOfExtracts; i++) {
      extractIDs
          .add(extractFactory.getUniqueExtract(Extract.TableType.APPEND_ONLY, "namespace", "table").getExtractId());
    }
    Assert.assertEquals(extractIDs.size(), numOfExtracts);
  }
}

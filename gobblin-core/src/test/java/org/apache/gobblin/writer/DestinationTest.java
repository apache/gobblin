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

package org.apache.gobblin.writer;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.State;


/**
 * Unit tests for {@link Destination}.
 *
 * @author Yinan Li
 */
@Test(groups = {"gobblin.writer"})
public class DestinationTest {

  @Test
  public void testMethods() {
    State state = new State();
    state.setProp("foo", "bar");
    Destination destination = Destination.of(Destination.DestinationType.HDFS, state);
    Assert.assertEquals(destination.getType(), Destination.DestinationType.HDFS);
    Assert.assertEquals(destination.getProperties().getPropertyNames().size(), 1);
    Assert.assertEquals(destination.getProperties().getProp("foo"), "bar");
  }
}

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

package org.apache.gobblin.metrics;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link Tag}.
 *
 * @author Yinan Li
 */
@Test(groups = {"gobblin.metrics"})
public class TagTest {

  private static final String JOB_ID_KEY = "job.id";
  private static final String JOB_ID = "TestJob-0";
  private static final String PROJECT_VERSION_KEY = "project.version";
  private static final int PROJECT_VERSION = 1;

  @Test
  public void testTags() {
    Tag<String> jobIdTag = new Tag<String>(JOB_ID_KEY, JOB_ID);
    Assert.assertEquals(jobIdTag.getKey(), JOB_ID_KEY);
    Assert.assertEquals(jobIdTag.getValue(), JOB_ID);

    Tag<Integer> projectVersionTag = new Tag<Integer>(PROJECT_VERSION_KEY, PROJECT_VERSION);
    Assert.assertEquals(projectVersionTag.getKey(), PROJECT_VERSION_KEY);
    Assert.assertEquals(projectVersionTag.getValue().intValue(), PROJECT_VERSION);
  }
}

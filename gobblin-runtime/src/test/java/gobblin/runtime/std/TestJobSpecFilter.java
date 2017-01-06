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
package gobblin.runtime.std;

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.runtime.api.JobSpec;

/**
 * Unit tests for {@link JobSpecFilter}
 */
public class TestJobSpecFilter {

  @Test public void testUriAndVersion() {
    JobSpec js1_1 = JobSpec.builder("gobblin:/test/job1").withVersion("1").build();
    JobSpec js1_2 = JobSpec.builder("gobblin:/test/job1").withVersion("2").build();
    JobSpec js2_1 = JobSpec.builder("gobblin:/test/job2").withVersion("1").build();
    JobSpec js2_2 = JobSpec.builder("gobblin:/test/job2").withVersion("2").build();

    JobSpecFilter filter1 = JobSpecFilter.eqJobSpecURI("gobblin:/test/job1");
    Assert.assertTrue(filter1.apply(js1_1));
    Assert.assertTrue(filter1.apply(js1_2));
    Assert.assertFalse(filter1.apply(js2_1));
    Assert.assertFalse(filter1.apply(js2_2));

    JobSpecFilter filter2 =
        JobSpecFilter.builder().eqURI("gobblin:/test/job2").eqVersion("2").build();
    Assert.assertFalse(filter2.apply(js1_1));
    Assert.assertFalse(filter2.apply(js1_2));
    Assert.assertFalse(filter2.apply(js2_1));
    Assert.assertTrue(filter2.apply(js2_2));
  }

}

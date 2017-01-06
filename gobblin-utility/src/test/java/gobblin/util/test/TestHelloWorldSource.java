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
package gobblin.util.test;

import java.io.IOException;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.source.workunit.WorkUnit;

/**
 * Unit tests for {@link HelloWorldSource}
 */
public class TestHelloWorldSource {

  @Test public void testSourceExtractor() throws DataRecordException, IOException {
    SourceState state = new SourceState();
    state.setProp(HelloWorldSource.NUM_HELLOS_FULL_KEY, 10);

    HelloWorldSource source = new HelloWorldSource();

    List<WorkUnit> wus = source.getWorkunits(state);
    Assert.assertEquals(wus.size(), 10);

    for (int i = 0; i < wus.size(); ++i) {
      WorkUnit wu = wus.get(i);
      Assert.assertEquals(wu.getPropAsInt(HelloWorldSource.HELLO_ID_FULL_KEY), i + 1);
      WorkUnitState wuState = new WorkUnitState(wu, state);
      Extractor<String, String> extr = source.getExtractor(wuState);

      Assert.assertEquals(extr.getExpectedRecordCount(), 1);
      Assert.assertEquals(extr.readRecord(null), "Hello world "+ (i+1) + " !");
    }
  }

}

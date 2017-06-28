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

package gobblin.source.extractor.filebased;

import java.io.File;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.embedded.EmbeddedGobblin;
import gobblin.writer.test.GobblinTestEventBusWriter;
import gobblin.writer.test.TestingEventBusAsserter;
import gobblin.writer.test.TestingEventBuses;

public class TextFileBasedSourceTest {

  @Test(groups = { "disabledOnTravis" })
  public void test() throws Exception {
    File stateStoreDir = Files.createTempDir();
    stateStoreDir.deleteOnExit();

    File dataDir = Files.createTempDir();
    dataDir.deleteOnExit();

    String eventBusId = UUID.randomUUID().toString();
    TestingEventBusAsserter asserter = new TestingEventBusAsserter(eventBusId);

    EmbeddedGobblin gobblin = new EmbeddedGobblin().setTemplate("resource:///templates/textFileBasedSourceTest.template")
        .setConfiguration(ConfigurationKeys.SOURCE_FILEBASED_DATA_DIRECTORY, dataDir.getAbsolutePath())
        .setConfiguration(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, stateStoreDir.getAbsolutePath())
        .setConfiguration(GobblinTestEventBusWriter.FULL_EVENTBUSID_KEY, eventBusId)
        .setConfiguration(ConfigurationKeys.STATE_STORE_ENABLED, "true");

    Files.write("record1\nrecord2\nrecord3", new File(dataDir, "file1"), Charsets.UTF_8);
    Files.write("record4\nrecord5", new File(dataDir, "file2"), Charsets.UTF_8);

    gobblin.run();

    Set<Object> events = asserter.getEvents().stream().map(TestingEventBuses.Event::getValue).collect(Collectors.toSet());
    Assert.assertEquals(events, Sets.newHashSet("record1", "record2", "record3", "record4", "record5"));
    asserter.clear();

    // should only pull new files
    Files.write("record6\nrecord7", new File(dataDir, "file3"), Charsets.UTF_8);

    gobblin.run();

    events = asserter.getEvents().stream().map(TestingEventBuses.Event::getValue).collect(Collectors.toSet());
    Assert.assertEquals(events, Sets.newHashSet("record6", "record7"));
    asserter.clear();

    // if we replace old file, it should repull that file
    Assert.assertTrue(new File(dataDir, "file2").delete());

    // Some systems don't provide modtime so gobblin can't keep of changed files.
    // run gobblin once with file2 deleted to update the snapshot
    gobblin.run();
    events = asserter.getEvents().stream().map(TestingEventBuses.Event::getValue).collect(Collectors.toSet());
    Assert.assertTrue(events.isEmpty());
    asserter.clear();

    Files.write("record8\nrecord9", new File(dataDir, "file2"), Charsets.UTF_8);

    gobblin.run();

    events = asserter.getEvents().stream().map(TestingEventBuses.Event::getValue).collect(Collectors.toSet());
    Assert.assertEquals(events, Sets.newHashSet("record8", "record9"));
    asserter.clear();

  }

  @Test
  public void testFileLimit() throws Exception {
    File stateStoreDir = Files.createTempDir();
    stateStoreDir.deleteOnExit();

    File dataDir = Files.createTempDir();
    dataDir.deleteOnExit();

    String eventBusId = UUID.randomUUID().toString();
    TestingEventBusAsserter asserter = new TestingEventBusAsserter(eventBusId);

    EmbeddedGobblin gobblin = new EmbeddedGobblin().setTemplate("resource:///templates/textFileBasedSourceTest.template")
        .setConfiguration(ConfigurationKeys.SOURCE_FILEBASED_DATA_DIRECTORY, dataDir.getAbsolutePath())
        .setConfiguration(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, stateStoreDir.getAbsolutePath())
        .setConfiguration(GobblinTestEventBusWriter.FULL_EVENTBUSID_KEY, eventBusId)
        .setConfiguration(ConfigurationKeys.STATE_STORE_ENABLED, "true")
        .setConfiguration(ConfigurationKeys.SOURCE_FILEBASED_MAX_FILES_PER_RUN, "2");

    Files.write("record1\nrecord2\nrecord3", new File(dataDir, "file1"), Charsets.UTF_8);
    Files.write("record4\nrecord5", new File(dataDir, "file2"), Charsets.UTF_8);
    Files.write("record6\nrecord7", new File(dataDir, "file3"), Charsets.UTF_8);


    gobblin.run();

    // should only pull first 2 files
    Set<Object> events = asserter.getEvents().stream().map(TestingEventBuses.Event::getValue).collect(Collectors.toSet());
    Assert.assertEquals(events, Sets.newHashSet("record1", "record2", "record3", "record4", "record5"));
    asserter.clear();

    gobblin.run();

    events = asserter.getEvents().stream().map(TestingEventBuses.Event::getValue).collect(Collectors.toSet());
    Assert.assertEquals(events, Sets.newHashSet("record6", "record7"));
    asserter.clear();
  }

}

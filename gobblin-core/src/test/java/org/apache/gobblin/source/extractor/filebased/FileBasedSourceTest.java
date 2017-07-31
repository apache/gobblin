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

package org.apache.gobblin.source.extractor.filebased;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.junit.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import java.io.IOException;
import java.util.List;

@Test
public class FileBasedSourceTest {
    @Test
    public void testFailJobWhenPreviousStateExistsButDoesNotHaveSnapshot() {
        try {
            DummyFileBasedSource source = new DummyFileBasedSource();

            WorkUnitState workUnitState = new WorkUnitState();
            workUnitState.setId("priorState");
            List<WorkUnitState> workUnitStates = Lists.newArrayList(workUnitState);

            State state = new State();
            state.setProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY, Extract.TableType.SNAPSHOT_ONLY.toString());
            state.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_PRIOR_SNAPSHOT_REQUIRED, true);

            SourceState sourceState = new SourceState(state, workUnitStates);

            source.getWorkunits(sourceState);
            Assert.fail("Expected RuntimeException, but no exceptions were thrown.");
        } catch (RuntimeException e) {
            Assert.assertEquals("No 'source.filebased.fs.snapshot' found on state of prior job",
                e.getMessage());
        }
    }

    private static class DummyFileBasedSource extends FileBasedSource<String, String> {
        @Override
        public void initFileSystemHelper(State state) throws FileBasedHelperException {
        }

        @Override
        protected List<WorkUnit> getPreviousWorkUnitsForRetry(SourceState state) {
            return Lists.newArrayList();
        }

        @Override
        public List<String> getcurrentFsSnapshot(State state) {
            return Lists.newArrayList("SnapshotEntry");
        }

        @Override
        public Extractor<String, String> getExtractor(WorkUnitState state) throws IOException {
            return new DummyExtractor();
        }
    }

    private static class DummyExtractor implements Extractor<String, String> {
        @Override
        public String getSchema() {
            return "";
        }

        @Override
        public String readRecord(String reuse) throws DataRecordException, IOException {
            return null;
        }

        @Override
        public long getExpectedRecordCount() {
            return 0;
        }

        @Override
        public long getHighWatermark() {
            return 0;
        }

        @Override
        public void close() throws IOException {}
    }
}

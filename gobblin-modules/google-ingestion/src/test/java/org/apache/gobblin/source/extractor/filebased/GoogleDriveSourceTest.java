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

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.extract.google.GoogleDriveExtractor;
import org.apache.gobblin.source.extractor.extract.google.GoogleDriveFsHelper;
import org.apache.gobblin.source.extractor.extract.google.GoogleDriveSource;


@Test(groups = { "gobblin.source.extractor.google" })
public class GoogleDriveSourceTest {

  @SuppressWarnings("unchecked")
  public void testGetcurrentFsSnapshot() throws FileBasedHelperException {
    @SuppressWarnings("rawtypes")
    GoogleDriveSource source = new GoogleDriveSource<>();
    GoogleDriveFsHelper fsHelper = mock(GoogleDriveFsHelper.class);
    source.fsHelper = fsHelper;

    List<String> fileIds = ImmutableList.of("test1", "test2", "test3");
    when(fsHelper.ls(anyString())).thenReturn(fileIds);
    long timestamp = System.currentTimeMillis();
    when(fsHelper.getFileMTime(anyString())).thenReturn(timestamp);

    List<String> expected = Lists.newArrayList();
    for (String fileId : fileIds) {
      expected.add(fileId + source.splitPattern + timestamp);
    }

    Assert.assertEquals(expected, source.getcurrentFsSnapshot(new State()));
  }

  public void testGetExtractor() throws IOException {
    @SuppressWarnings("rawtypes")
    GoogleDriveSource source = new GoogleDriveSource<>();
    GoogleDriveFsHelper fsHelper = mock(GoogleDriveFsHelper.class);
    source.fsHelper = fsHelper;
    Extractor extractor = source.getExtractor(new WorkUnitState());

    Assert.assertTrue(extractor instanceof GoogleDriveExtractor);
  }
}

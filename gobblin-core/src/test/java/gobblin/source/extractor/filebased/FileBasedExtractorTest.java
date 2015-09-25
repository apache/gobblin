/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor.filebased;

import java.io.IOException;

import com.google.common.base.Joiner;

import org.apache.commons.io.IOUtils;

import org.mockito.Mockito;

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;


@Test
public class FileBasedExtractorTest {

  public void testReadRecordWithNoFiles() throws DataRecordException, IOException {
    WorkUnitState state = new WorkUnitState();
    state.setProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL, "");

    FileBasedHelper fsHelper = Mockito.mock(FileBasedHelper.class);
    FileBasedExtractor<String, String> extractor = new DummyFileBasedExtractor<String, String>(state, fsHelper);

    Assert.assertEquals(getNumRecords(extractor), 0);
  }

  public void testReadRecordWithEmptyFiles() throws DataRecordException, IOException, FileBasedHelperException {
    String file1 = "file1.txt";
    String file2 = "file2.txt";
    String file3 = "file3.txt";

    WorkUnitState state = new WorkUnitState();
    state.setProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL, Joiner.on(",").join(file1, file2, file3));

    FileBasedHelper fsHelper = Mockito.mock(FileBasedHelper.class);
    Mockito.when(fsHelper.getFileStream(file1)).thenReturn(IOUtils.toInputStream(""));
    Mockito.when(fsHelper.getFileStream(file2)).thenReturn(IOUtils.toInputStream(""));
    Mockito.when(fsHelper.getFileStream(file3)).thenReturn(IOUtils.toInputStream(""));

    FileBasedExtractor<String, String> extractor = new DummyFileBasedExtractor<String, String>(state, fsHelper);

    Assert.assertEquals(getNumRecords(extractor), 0);
  }

  public void testReadRecordWithNonEmptyFiles() throws DataRecordException, IOException, FileBasedHelperException {
    String file1 = "file1.txt";
    String file2 = "file2.txt";
    String file3 = "file3.txt";

    WorkUnitState state = new WorkUnitState();
    state.setProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL, Joiner.on(",").join(file1, file2, file3));

    FileBasedHelper fsHelper = Mockito.mock(FileBasedHelper.class);
    Mockito.when(fsHelper.getFileStream(file1)).thenReturn(IOUtils.toInputStream("record1 \n record2"));
    Mockito.when(fsHelper.getFileStream(file2)).thenReturn(IOUtils.toInputStream("record3 \n record4"));
    Mockito.when(fsHelper.getFileStream(file3)).thenReturn(IOUtils.toInputStream("record5 \n record6 \n record7"));

    FileBasedExtractor<String, String> extractor = new DummyFileBasedExtractor<String, String>(state, fsHelper);

    Assert.assertEquals(getNumRecords(extractor), 7);
  }

  public void testReadRecordWithEmptyAndNonEmptyFiles() throws DataRecordException, IOException, FileBasedHelperException {
    String file1 = "file1.txt";
    String file2 = "file2.txt";
    String file3 = "file3.txt";

    WorkUnitState state = new WorkUnitState();
    state.setProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL, Joiner.on(",").join(file1, file2, file3));

    FileBasedHelper fsHelper = Mockito.mock(FileBasedHelper.class);
    Mockito.when(fsHelper.getFileStream(file1)).thenReturn(IOUtils.toInputStream("record1 \n record2"));
    Mockito.when(fsHelper.getFileStream(file2)).thenReturn(IOUtils.toInputStream(""));
    Mockito.when(fsHelper.getFileStream(file3)).thenReturn(IOUtils.toInputStream("record3 \n record4 \n record5"));

    FileBasedExtractor<String, String> extractor = new DummyFileBasedExtractor<String, String>(state, fsHelper);

    Assert.assertEquals(getNumRecords(extractor), 5);
  }

  private int getNumRecords(Extractor<?, ?> extractor) throws DataRecordException, IOException {
    int numRecords = 0;
    while (extractor.readRecord(null) != null) {
      numRecords++;
    }
    return numRecords;
  }

  private static class DummyFileBasedExtractor<S, D> extends FileBasedExtractor<S, D> {

    public DummyFileBasedExtractor(WorkUnitState workUnitState, FileBasedHelper fsHelper) {
      super(workUnitState, fsHelper);
    }
  }
}

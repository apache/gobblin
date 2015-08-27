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

package gobblin.data.management.trash;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class MockTrashTest {

  @Test
  public void MockTrashTest() throws IOException {

    FileSystem fs = mock(FileSystem.class);

    Path homeDirectory = new Path("/home/directory");
    when(fs.getHomeDirectory()).thenReturn(homeDirectory);
    when(fs.makeQualified(any(Path.class))).thenAnswer(new Answer<Path>() {
      @Override
      public Path answer(InvocationOnMock invocation)
          throws Throwable {
        return (Path) invocation.getArguments()[0];
      }
    });

    Trash trash = new MockTrash(fs, new Properties(), "user");

    Assert.assertTrue(trash.moveToTrash(new Path("/some/path")));

    verify(fs).getHomeDirectory();
    verify(fs).makeQualified(any(Path.class));
    verifyNoMoreInteractions(fs);

  }

}

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

package org.apache.gobblin.util.filesystem;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.util.limiter.CountBasedLimiter;
import org.apache.gobblin.util.limiter.Limiter;
import org.apache.gobblin.util.limiter.NotEnoughPermitsException;


public class ThrottledFileSystemTest {

  @Test
  public void testSimpleCalls() throws Exception {
    FileSystem fs = Mockito.mock(FileSystem.class);
    Mockito.when(fs.getFileStatus(Mockito.any(Path.class))).thenReturn(new FileStatus(0, false, 0, 0, 0, new Path("/")));

    Limiter limiter = new CountBasedLimiter(2);

    ThrottledFileSystem throttledFileSystem = new ThrottledFileSystem(fs, limiter, "testService");

    Assert.assertNotNull(throttledFileSystem.getFileStatus(new Path("/myFile")));
    Assert.assertNotNull(throttledFileSystem.getFileStatus(new Path("/myFile")));
    try {
      throttledFileSystem.getFileStatus(new Path("/myFile"));
      Assert.fail();
    } catch (NotEnoughPermitsException expected) {
      // Expected
    }

  }

  @Test
  public void testListing() throws Exception {
    FileSystem fs = Mockito.mock(FileSystem.class);
    Mockito.when(fs.listStatus(Mockito.any(Path.class))).thenAnswer(new Answer<FileStatus[]>() {
      @Override
      public FileStatus[] answer(InvocationOnMock invocation) throws Throwable {
        Path path = (Path) invocation.getArguments()[0];
        int files = Integer.parseInt(path.getName());

        FileStatus status = new FileStatus(0, false, 0, 0, 0, new Path("/"));
        FileStatus[] out = new FileStatus[files];
        for (int i = 0; i < files; i++) {
          out[i] = status;
        }

        return out;
      }
    });

    Limiter limiter = new CountBasedLimiter(5);

    ThrottledFileSystem throttledFileSystem = new ThrottledFileSystem(fs, limiter, "testService");
    Assert.assertEquals(throttledFileSystem.getServiceName(), "testService");

    Assert.assertNotNull(throttledFileSystem.listStatus(new Path("/files/99"))); // use 1 permit
    Assert.assertNotNull(throttledFileSystem.listStatus(new Path("/files/250"))); // use 3 permits
    try {
      throttledFileSystem.listStatus(new Path("/files/150")); // requires 2 permits
      Assert.fail();
    } catch (NotEnoughPermitsException expected) {
      // Expected
    }
    Assert.assertNotNull(throttledFileSystem.listStatus(new Path("/files/99"))); // requires 1 permit
  }

}

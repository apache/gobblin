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

import java.io.IOException;
import org.apache.gobblin.configuration.State;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test {@link FsDataWriter}
 */
public class FsDataWriterTest {

  @Test
  public void testExceptionOnGetRecordsWritten() throws IOException {
    FsDataWriter writer = Mockito.mock(FsDataWriter.class);
    final long BYTES_WRITTEN = 1000;

    Mockito.when(writer.getFinalState()).thenCallRealMethod();
    Mockito.when(writer.recordsWritten()).thenThrow(new RuntimeException("Test exception"));
    Mockito.when(writer.bytesWritten()).thenReturn(BYTES_WRITTEN);


    State finalState = writer.getFinalState();

    Assert.assertNull(finalState.getProp("RecordsWritten"));
    Assert.assertEquals(finalState.getPropAsLong("BytesWritten"), BYTES_WRITTEN);
  }

  @Test
  public void testExceptionOnGetBytesWritten() throws IOException {
    FsDataWriter writer = Mockito.mock(FsDataWriter.class);
    final long RECORDS_WRITTEN = 1000;

    Mockito.when(writer.getFinalState()).thenCallRealMethod();
    Mockito.when(writer.bytesWritten()).thenThrow(new RuntimeException("Test exception"));
    Mockito.when(writer.recordsWritten()).thenReturn(RECORDS_WRITTEN);


    State finalState = writer.getFinalState();

    Assert.assertNull(finalState.getProp("BytesWritten"));
    Assert.assertEquals(finalState.getPropAsLong("RecordsWritten"), RECORDS_WRITTEN);
  }
}

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
package org.apache.gobblin.eventhub.writer;

import java.io.IOException;

import org.apache.gobblin.writer.BytesBoundedBatch;
import org.apache.gobblin.writer.LargeMessagePolicy;
import org.apache.gobblin.writer.RecordTooLargeException;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.writer.WriteCallback;


public class EventhubBatchTest {

  @Test
  public void testBatchWithLargeRecord()
      throws IOException, RecordTooLargeException {
    // Assume memory size has only 2 bytes
    BytesBoundedBatch batch = new BytesBoundedBatch(8, 3000);

    String record = "abcdefgh";

    // Record is larger than the memory size limit, the first append should fail
    Assert.assertNull(batch.tryAppend(record, WriteCallback.EMPTY, LargeMessagePolicy.DROP));

    // The second append should still fail
    Assert.assertNull(batch.tryAppend(record, WriteCallback.EMPTY, LargeMessagePolicy.DROP));
  }

  @Test
  public void testBatch()
      throws IOException, RecordTooLargeException {
    // Assume memory size has only 200 bytes
    BytesBoundedBatch batch = new BytesBoundedBatch(200, 3000);

    // Add additional 15 bytes overhead, total size is 27 bytes
    String record = "abcdefgh";

    LargeMessagePolicy policy = LargeMessagePolicy.DROP;
    Assert.assertNotNull(batch.tryAppend(record, WriteCallback.EMPTY, policy));
    Assert.assertNotNull(batch.tryAppend(record, WriteCallback.EMPTY, policy));
    Assert.assertNotNull(batch.tryAppend(record, WriteCallback.EMPTY, policy));
    Assert.assertNotNull(batch.tryAppend(record, WriteCallback.EMPTY, policy));
    Assert.assertNotNull(batch.tryAppend(record, WriteCallback.EMPTY, policy));
    Assert.assertNotNull(batch.tryAppend(record, WriteCallback.EMPTY, policy));
    Assert.assertNotNull(batch.tryAppend(record, WriteCallback.EMPTY, policy));

    // Batch has room for 8th record
    Assert.assertEquals(batch.hasRoom(record, policy), true);
    Assert.assertNotNull(batch.tryAppend(record, WriteCallback.EMPTY, policy));

    // Batch has no room for 9th record
    Assert.assertEquals(batch.hasRoom(record, policy), false);
    Assert.assertNull(batch.tryAppend(record, WriteCallback.EMPTY, policy));
  }
}

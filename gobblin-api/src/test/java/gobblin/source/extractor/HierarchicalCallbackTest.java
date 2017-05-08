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

package gobblin.source.extractor;

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.writer.Ackable;


public class HierarchicalCallbackTest {

  @Test
  public void testCloseBeforeAck() {

    MyAckable ackable = new MyAckable();

    HierarchicalCallback hierarchicalCallback = new HierarchicalCallback(ackable);
    Callback callback1 = hierarchicalCallback.newChildCallback();

    Assert.assertFalse(ackable.acked);
    callback1.onSuccess();
    Assert.assertFalse(ackable.acked);

    Callback callback2 = hierarchicalCallback.newChildCallback();
    hierarchicalCallback.close();
    Assert.assertFalse(ackable.acked);
    callback2.onSuccess();
    Assert.assertTrue(ackable.acked);
  }

  @Test
  public void testAckBeforeClose() {

    MyAckable ackable = new MyAckable();

    HierarchicalCallback hierarchicalCallback = new HierarchicalCallback(ackable);
    Callback callback1 = hierarchicalCallback.newChildCallback();

    Assert.assertFalse(ackable.acked);
    callback1.onSuccess();
    Assert.assertFalse(ackable.acked);

    Callback callback2 = hierarchicalCallback.newChildCallback();
    Assert.assertFalse(ackable.acked);
    callback2.onSuccess();
    Assert.assertFalse(ackable.acked);
    hierarchicalCallback.close();
    Assert.assertTrue(ackable.acked);
  }

  private static class MyAckable implements Ackable {
    private boolean acked = false;
    @Override
    public void ack() {
      this.acked = true;
    }
  }

}

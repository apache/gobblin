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

package org.apache.gobblin.stream;

import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.ack.BasicAckableForTesting;


public class StreamEntityTest {

  @Test
  public void testAcking() {
    MyStreamEntity streamEntity = new MyStreamEntity();

    BasicAckableForTesting ackable1 = new BasicAckableForTesting();
    streamEntity.addCallBack(ackable1);
    BasicAckableForTesting ackable2 = new BasicAckableForTesting();
    streamEntity.addCallBack(ackable2);

    streamEntity.ack();
    Assert.assertEquals(ackable1.acked, 1);
    Assert.assertEquals(ackable2.acked, 1);
  }

  @Test
  public void testSingleCloning() {
    MyStreamEntity streamEntity = new MyStreamEntity();

    BasicAckableForTesting ackable = new BasicAckableForTesting();
    streamEntity.addCallBack(ackable);

    MyStreamEntity clone = (MyStreamEntity) streamEntity.getSingleClone();
    Assert.assertEquals(clone.id, streamEntity.id);

    try {
      streamEntity.getSingleClone();
      Assert.fail();
    } catch (IllegalStateException ise) {
      // expected, cannot clone twice using getSingleClone
    }

    try {
      streamEntity.forkCloner();
      Assert.fail();
    } catch (IllegalStateException ise) {
      // expected, cannot clone twice
    }

    clone.ack();
    Assert.assertEquals(ackable.acked, 1);
  }

  @Test
  public void testMultipleClones() {
    MyStreamEntity streamEntity = new MyStreamEntity();

    BasicAckableForTesting ackable = new BasicAckableForTesting();
    streamEntity.addCallBack(ackable);

    StreamEntity.ForkCloner cloner = streamEntity.forkCloner();

    MyStreamEntity clone1 = (MyStreamEntity) cloner.getClone();
    Assert.assertEquals(clone1.id, streamEntity.id);
    clone1.ack();
    // cloner has not been closed, so ack does not spread
    Assert.assertEquals(ackable.acked, 0);

    MyStreamEntity clone2 = (MyStreamEntity) cloner.getClone();
    Assert.assertEquals(clone2.id, streamEntity.id);

    // close cloner to spread acks
    cloner.close();

    // ack second clone, should ack original
    clone2.ack();
    Assert.assertEquals(ackable.acked, 1);

    try {
      cloner.getClone();
      Assert.fail();
    } catch (IllegalStateException ise) {
      // cloner has been closed, cannot create new clones
    }

    try {
      streamEntity.getSingleClone();
      Assert.fail();
    } catch (IllegalStateException ise) {
      // expected, cannot clone twice
    }

    try {
      streamEntity.forkCloner();
      Assert.fail();
    } catch (IllegalStateException ise) {
      // expected, cannot clone twice
    }
  }

  @Test
  public void testNack() {
    MyStreamEntity streamEntity = new MyStreamEntity();

    BasicAckableForTesting ackable = new BasicAckableForTesting();
    streamEntity.addCallBack(ackable);

    streamEntity.nack(new RuntimeException());

    Assert.assertEquals(ackable.nacked, 1);
    Assert.assertTrue(ackable.throwable instanceof RuntimeException);
  }

  public static class MyStreamEntity extends StreamEntity<String> {
    private final int id;

    public MyStreamEntity() {
      this.id = new Random().nextInt();
    }

    public MyStreamEntity(int id) {
      this.id = id;
    }

    @Override
    protected StreamEntity<String> buildClone() {
      return new MyStreamEntity(this.id);
    }
  }

}

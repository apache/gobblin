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
package org.apache.gobblin.util.callbacks;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Function;

import lombok.AllArgsConstructor;

/**
 * Unit tests for {@link CallbacksDispatcher}
 */
public class TestCallbacksDispatcher {

  @Test
  public void testHappyPath() throws InterruptedException, ExecutionException {
    final Logger log =
        LoggerFactory.getLogger(TestCallbacksDispatcher.class.getSimpleName() +
                               ".testHappyPath");
    CallbacksDispatcher<MyListener> disp1 = new CallbacksDispatcher<>(log);
    runHappyPath(disp1);

    CallbacksDispatcher<MyListener> disp2 =
        new CallbacksDispatcher<>(Executors.newFixedThreadPool(2), log);
    runHappyPath(disp2);
  }

  @Test
  public void testErrorCallback() throws InterruptedException, ExecutionException {
    final Logger log =
        LoggerFactory.getLogger(TestCallbacksDispatcher.class.getSimpleName() +
                               ".testErrorCallback");
    CallbacksDispatcher<MyListener> disp1 = new CallbacksDispatcher<>(log);
    MyListener l1 = Mockito.mock(MyListener.class);
    MyListener l2 = Mockito.mock(MyListener.class);
    MyListener l3 = Mockito.mock(MyListener.class);

    Mockito.doThrow(new RuntimeException("injected error")).when(l2).voidCallback();

    disp1.addListener(l1);
    disp1.addListener(l2);
    disp1.addListener(l3);

    final VoidCallback voidCallback = new VoidCallback();
    CallbacksDispatcher.CallbackResults<MyListener, Void> voidRes =
        disp1.execCallbacks(voidCallback);
    Assert.assertEquals(voidRes.getSuccesses().size(), 2);
    Assert.assertEquals(voidRes.getFailures().size(), 1);
    Assert.assertEquals(voidRes.getCancellations().size(), 0);
    Assert.assertTrue(voidRes.getSuccesses().get(l1).isSuccessful());
    Assert.assertTrue(voidRes.getSuccesses().get(l3).isSuccessful());
    Assert.assertTrue(voidRes.getFailures().get(l2).hasFailed());
  }

  private void runHappyPath(CallbacksDispatcher<MyListener> disp) throws InterruptedException, ExecutionException {
    MyListener l1 = Mockito.mock(MyListener.class);
    MyListener l2 = Mockito.mock(MyListener.class);

    Mockito.when(l1.booleanCallback(Mockito.eq(1))).thenReturn(true);
    Mockito.when(l1.booleanCallback(Mockito.eq(2))).thenReturn(false);
    Mockito.when(l2.booleanCallback(Mockito.eq(1))).thenReturn(false);
    Mockito.when(l2.booleanCallback(Mockito.eq(2))).thenReturn(true);


    final VoidCallback voidCallback = new VoidCallback();
    final BoolCallback boolCallback1 = new BoolCallback(1);
    final BoolCallback boolCallback2 = new BoolCallback(2);

    Assert.assertEquals(disp.getListeners().size(), 0);
    CallbacksDispatcher.CallbackResults<MyListener, Void> voidRes = disp.execCallbacks(voidCallback);
    Assert.assertEquals(voidRes.getSuccesses().size(), 0);
    Assert.assertEquals(voidRes.getFailures().size(), 0);
    Assert.assertEquals(voidRes.getCancellations().size(), 0);

    disp.addListener(l1);
    Assert.assertEquals(disp.getListeners().size(), 1);
    voidRes = disp.execCallbacks(voidCallback);
    Assert.assertEquals(voidRes.getSuccesses().size(), 1);
    Assert.assertEquals(voidRes.getFailures().size(), 0);
    Assert.assertEquals(voidRes.getCancellations().size(), 0);

    disp.addListener(l2);
    Assert.assertEquals(disp.getListeners().size(), 2);
    CallbacksDispatcher.CallbackResults<MyListener, Boolean> boolRes = disp.execCallbacks(boolCallback1);
    Assert.assertEquals(boolRes.getSuccesses().size(), 2);
    Assert.assertEquals(boolRes.getFailures().size(), 0);
    Assert.assertEquals(boolRes.getCancellations().size(), 0);
    Assert.assertTrue(boolRes.getSuccesses().get(l1).getResult());
    Assert.assertFalse(boolRes.getSuccesses().get(l2).getResult());

    disp.removeListener(l1);
    Assert.assertEquals(disp.getListeners().size(), 1);
    boolRes = disp.execCallbacks(boolCallback2);
    Assert.assertEquals(boolRes.getSuccesses().size(), 1);
    Assert.assertEquals(boolRes.getFailures().size(), 0);
    Assert.assertEquals(boolRes.getCancellations().size(), 0);
    Assert.assertTrue(boolRes.getSuccesses().get(l2).getResult());

    disp.removeListener(l2);
    Assert.assertEquals(disp.getListeners().size(), 0);
    boolRes = disp.execCallbacks(boolCallback2);
    Assert.assertEquals(boolRes.getSuccesses().size(), 0);
    Assert.assertEquals(boolRes.getFailures().size(), 0);
    Assert.assertEquals(boolRes.getCancellations().size(), 0);

    Mockito.verify(l1).voidCallback();
    Mockito.verify(l1).booleanCallback(Mockito.eq(1));
    Mockito.verify(l2).booleanCallback(Mockito.eq(1));
    Mockito.verify(l2).booleanCallback(Mockito.eq(2));
  }

  private static interface MyListener {
    void voidCallback();
    boolean booleanCallback(int param);
  }

  private static class VoidCallback implements Function<MyListener, Void> {
    @Override public Void apply(MyListener input) {
      input.voidCallback();
      return null;
    }
  }

  @AllArgsConstructor
  private static class BoolCallback implements Function<MyListener, Boolean> {
    private final int param;

    @Override public Boolean apply(MyListener input) {
      return input.booleanCallback(param);
    }
  }

}

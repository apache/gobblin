/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util;

import java.util.concurrent.ThreadFactory;

import org.mockito.Mockito;
import org.slf4j.Logger;
import org.testng.annotations.Test;

import com.google.common.base.Optional;


/**
 * Unit tests for {@link ExecutorsUtils}.
 *
 * @author ynli
 */
@Test(groups = {"gobblin.util"})
public class ExecutorsUtilsTest {

  @Test(expectedExceptions = RuntimeException.class)
  public void testNewThreadFactory() throws InterruptedException {
    Logger logger = Mockito.mock(Logger.class);

    ThreadFactory threadFactory = ExecutorsUtils.newThreadFactory(Optional.of(logger));
    final RuntimeException runtimeException = new RuntimeException();
    Thread thread = threadFactory.newThread(new Runnable() {
      @Override
      public void run() {
        throw runtimeException;
      }
    });
    thread.setName("foo");
    String errorMessage = String.format("Thread %s threw an uncaught exception: %s", thread, runtimeException);
    Mockito.doThrow(runtimeException).when(logger).error(errorMessage, runtimeException);

    thread.run();
  }
}

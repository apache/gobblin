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

package org.apache.gobblin.util;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.assertj.core.util.Lists;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class ExceptionUtilsTest {
  @Test
  public void testIsInstanceOf() {
    List<Class<? extends Exception>> excpetionsList = Lists.list(ArithmeticException.class, SQLException.class);
    Exception e1 = new ArithmeticException();
    Exception e2 = new IOException(e1);
    Exception e3 = new RuntimeException(e2);
    Exception e4 = new UnsupportedOperationException();
    Exception e5 = new IOException(e4);
    Assert.assertTrue(ExceptionUtils.isExceptionInstanceOf(e1, excpetionsList));
    Assert.assertTrue(ExceptionUtils.isExceptionInstanceOf(e2, excpetionsList));
    Assert.assertTrue(ExceptionUtils.isExceptionInstanceOf(e3, excpetionsList));
    Assert.assertFalse(ExceptionUtils.isExceptionInstanceOf(e4, excpetionsList));
    Assert.assertFalse(ExceptionUtils.isExceptionInstanceOf(e5, excpetionsList));
  }

  @Test
  public void canRemoveEmptyWrapper() {
    Exception exception = new IOException(new IllegalArgumentException("root cause"));
    Throwable rootCause = ExceptionUtils.removeEmptyWrappers(exception);
    assertEquals(rootCause.getClass(), IllegalArgumentException.class);
  }

  @Test
  public void canRemoveMultipleEmptyWrappers() {
    Exception exception = new IOException(new IOException(new IllegalArgumentException("root cause")));
    Throwable unwrapped = ExceptionUtils.removeEmptyWrappers(exception);
    assertEquals(unwrapped.getClass(), IllegalArgumentException.class);
  }

  @Test
  public void willNotRemoveExceptionWithMessage() {
    Exception exception = new IOException("test message", new IllegalArgumentException("root cause"));
    Throwable unwrapped = ExceptionUtils.removeEmptyWrappers(exception);
    assertEquals(unwrapped.getClass(), IOException.class);
  }
}

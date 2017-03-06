/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package gobblin.fork;

import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Test
public class CopyHelperTest {

  private static final Random RANDOM = new Random();

  @Test
  public void testCopyable()
      throws CopyNotSupportedException {

    Copyable c = mock(Copyable.class);
    Assert.assertTrue(CopyHelper.isCopyable(c));

    Object copy = new Object();
    when(c.copy()).thenReturn(copy);
    Assert.assertEquals(CopyHelper.copy(c), copy);

  }

  @Test
  public void testByteArray()
      throws CopyNotSupportedException {

    int length = RANDOM.nextInt(200);
    byte[] bytes = new byte[length];
    RANDOM.nextBytes(bytes);

    Assert.assertTrue(CopyHelper.isCopyable(bytes));

    byte[] copiedBytes = (byte[]) CopyHelper.copy(bytes);
    Assert.assertTrue(copiedBytes != bytes, "Copied bytes reference should be different");
    Assert.assertEquals(copiedBytes, bytes, "Copied bytes value should be the same");
  }

  @Test
  public void testInteger()
      throws CopyNotSupportedException {

    Integer integer = RANDOM.nextInt(200);

    Assert.assertTrue(CopyHelper.isCopyable(integer));

    Integer copiedInteger = (Integer) CopyHelper.copy(integer);
    Assert.assertEquals(copiedInteger, integer, "Copied integer value should be the same");

  }

  @Test
  public void testString()
      throws CopyNotSupportedException {

    int length = RANDOM.nextInt(200);
    byte[] bytes = new byte[length];
    RANDOM.nextBytes(bytes);

    String string = new String(bytes);
    Assert.assertTrue(CopyHelper.isCopyable(string));

    String copiedString = (String) CopyHelper.copy(string);
    Assert.assertEquals(copiedString, string, "Copied string value should be the same");
  }


}

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

package org.apache.gobblin.util.io;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;

import org.apache.gobblin.util.limiter.CountBasedLimiter;
import org.apache.gobblin.util.limiter.Limiter;


public class ThrottledInputStreamTest {

  @Test
  public void test() throws Exception {
    ByteArrayInputStream inputStream = new ByteArrayInputStream("abcde".getBytes(Charsets.UTF_8));
    MeteredInputStream meteredInputStream = MeteredInputStream.builder().in(inputStream).updateFrequency(1).build();
    Limiter limiter = new CountBasedLimiter(4);

    InputStream throttled = new ThrottledInputStream(meteredInputStream, limiter, meteredInputStream);
    try {
      String output = IOUtils.toString(throttled, Charsets.UTF_8);
      Assert.fail();
    } catch (RuntimeException re) {
      // Expected
    }

    meteredInputStream.reset();
    limiter = new CountBasedLimiter(5);
    throttled = new ThrottledInputStream(meteredInputStream, limiter, meteredInputStream);

    Assert.assertEquals(IOUtils.toString(throttled, Charsets.UTF_8), "abcde");
  }

}

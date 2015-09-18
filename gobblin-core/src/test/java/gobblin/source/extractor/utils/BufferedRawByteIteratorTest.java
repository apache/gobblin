/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor.utils;

import java.io.ByteArrayInputStream;

import org.testng.Assert;
import org.testng.annotations.Test;


public class BufferedRawByteIteratorTest {

  private static final int ARRAY_LENGTH = 12352;
  private static final int BUFFER_LENGTH = 100;

  @Test public void testIterator() throws Exception {

    byte[] bytes = new byte[ARRAY_LENGTH];
    for(int i = 0; i < ARRAY_LENGTH; i++) {
      bytes[i] = (byte)(i % 256);
    }

    ByteArrayInputStream is = new ByteArrayInputStream(bytes);

    BufferedRawByteIterator it = new BufferedRawByteIterator(is, BUFFER_LENGTH);

    int count = 0;
    while(it.hasNext()) {
      byte[] nextChunk = it.next();
      Assert.assertEquals(nextChunk[0], bytes[count]);
      Assert.assertTrue(nextChunk.length <= BUFFER_LENGTH);
      count += nextChunk.length;
    }

    Assert.assertEquals(count, ARRAY_LENGTH);

  }
}

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

package gobblin.source.extractor.filebased;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;


/**
 * Test for {@link TokenizedFileDownloader}.
 */
public class TokenizedFileDownloaderTest {

  @Test
  public void testRecordIterator()
      throws UnsupportedEncodingException {
    String charset = "UTF-8";
    String delimiter = "\n\r";
    String record1 = "record1";
    String record2 = "record2\n";
    String record3 = "record3\r";
    InputStream inputStream =
        new ByteArrayInputStream(Joiner.on(delimiter).join(record1, record2, record3).getBytes(charset));

    TokenizedFileDownloader.RecordIterator recordIterator =
        new TokenizedFileDownloader.RecordIterator(inputStream, delimiter, charset);
    Assert.assertTrue(recordIterator.hasNext());
    Assert.assertEquals(recordIterator.next(), record1);
    Assert.assertTrue(recordIterator.hasNext());
    Assert.assertEquals(recordIterator.next(), record2);
    Assert.assertTrue(recordIterator.hasNext());
    Assert.assertEquals(recordIterator.next(), record3);
    Assert.assertFalse(recordIterator.hasNext());
  }
}

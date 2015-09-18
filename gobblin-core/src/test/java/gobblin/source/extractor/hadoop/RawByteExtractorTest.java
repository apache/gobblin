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

package gobblin.source.extractor.hadoop;

import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;


public class RawByteExtractorTest {

  @Test public void testExtractor() throws Exception {

    String plainTextPath = getClass().getClassLoader().getResource("plaintext").getFile();

    RawByteExtractor extractor = new RawByteExtractor(FileSystem.getLocal(new Configuration()),
        new Path(plainTextPath, "shakespeare.txt"));

    Assert.assertEquals(extractor.getExpectedRecordCount(), 11);

    String record = new String(extractor.readRecord(null), Charset.forName(Charsets.UTF_8.name()));

    Assert.assertTrue(record.startsWith("THE TRAGEDY OF ROMEO AND JULIET"));

    int records = 1;
    while(true) {
      byte[] nextByteArray = extractor.readRecord(null);
      if(nextByteArray == null) {
        break;
      }
      records++;
      record = new String(nextByteArray, Charset.forName(Charsets.UTF_8.name()));
    }

    Assert.assertTrue(record.contains("O, now be gone! More light and light it grows."));

  }
}

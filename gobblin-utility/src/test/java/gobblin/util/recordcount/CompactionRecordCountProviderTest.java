/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util.recordcount;

import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link CompactionRecordCountProvider}.
 */
@Test(groups = { "gobblin.util.recordcount" })
public class CompactionRecordCountProviderTest {
  @Test
  public void testFileNameRecordCountProvider() {
    CompactionRecordCountProvider filenameRecordCountProvider = new CompactionRecordCountProvider();

    Pattern pattern = Pattern.compile("part\\-r\\-123\\.[\\d]*\\.[\\d]*\\.avro");
    Assert.assertTrue(pattern.matcher(filenameRecordCountProvider.constructFileName("part-r-", 123)).matches());
    Assert.assertEquals(filenameRecordCountProvider.getRecordCount(new Path("part-r-123.1.2.avro")), 123);
  }
}

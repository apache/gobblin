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

package gobblin.writer;

import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link AvroHdfsTimePartitionedWithRecordCountsWriter.FilenameRecordCountProvider}.
 */
@Test(groups = { "gobblin.writer" })
public class AvroHdfsTimePartitionedWithRecordCountsWriterFileNameFormatTest {
  @Test
  public void testFileNameFormat() {
    AvroHdfsTimePartitionedWithRecordCountsWriter.FilenameRecordCountProvider filenameRecordCountProvider =
        new AvroHdfsTimePartitionedWithRecordCountsWriter.FilenameRecordCountProvider();
   Assert.assertEquals(filenameRecordCountProvider.constructFilePath("/a/b/c.avro", 123), "/a/b/c.123.avro");
   Assert.assertEquals(filenameRecordCountProvider.getRecordCount(new Path("/a/b/c.123.avro")), 123);
  }
}

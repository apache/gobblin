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
package gobblin.data.management.copy.extractor;

import gobblin.data.management.copy.CopyConfiguration;
import gobblin.data.management.copy.CopyContext;
import gobblin.data.management.copy.CopyableFile;
import gobblin.data.management.copy.FileAwareInputStream;
import gobblin.data.management.copy.PreserveAttributes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;


public class InputStreamExtractorTest {

  @Test
  public void testReadRecord() throws Exception {
    List<CopyableFile> files = new ArrayList<CopyableFile>();
    files.add(getTestCopyableFile("inputStreamExtractorTest/first.txt"));
    files.add(getTestCopyableFile("inputStreamExtractorTest/second.txt"));

    FileAwareInputStreamExtractor extractor =
        new FileAwareInputStreamExtractor(FileSystem.getLocal(new Configuration()), Lists.newArrayList(files).iterator());

    FileAwareInputStream fileAwareInputStream = extractor.readRecord(null);

    Assert.assertEquals(fileAwareInputStream.getFile().getOrigin().getPath(), files.get(0).getOrigin().getPath());
    Assert.assertEquals(IOUtils.toString(fileAwareInputStream.getInputStream()), "first");

    fileAwareInputStream = extractor.readRecord(null);

    Assert.assertEquals(fileAwareInputStream.getFile().getOrigin().getPath(), files.get(1).getOrigin().getPath());
    Assert.assertEquals(IOUtils.toString(fileAwareInputStream.getInputStream()), "second");
  }

  private CopyableFile getTestCopyableFile(String resourcePath) throws IOException {
    String filePath = getClass().getClassLoader().getResource(resourcePath).getFile();
    FileStatus status = new FileStatus(0l, false, 0, 0l, 0l, new Path(filePath));
    return CopyableFile.builder(FileSystem.getLocal(new Configuration()), status, new Path("/"),
        new CopyConfiguration(new Path("/"), PreserveAttributes.fromMnemonicString(""), new CopyContext())).build();
  }
}

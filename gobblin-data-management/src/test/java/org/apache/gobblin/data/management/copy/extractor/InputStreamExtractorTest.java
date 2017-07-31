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
package org.apache.gobblin.data.management.copy.extractor;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.data.management.copy.CopyConfiguration;
import org.apache.gobblin.data.management.copy.CopyContext;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.gobblin.data.management.copy.FileAwareInputStream;
import org.apache.gobblin.data.management.copy.PreserveAttributes;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;


public class InputStreamExtractorTest {

  @Test
  public void testReadRecord() throws Exception {
    CopyableFile file = getTestCopyableFile("inputStreamExtractorTest/first.txt");

    FileAwareInputStreamExtractor extractor =
        new FileAwareInputStreamExtractor(FileSystem.getLocal(new Configuration()), file);

    FileAwareInputStream fileAwareInputStream = extractor.readRecord(null);

    Assert.assertEquals(fileAwareInputStream.getFile().getOrigin().getPath(), file.getOrigin().getPath());
    Assert.assertEquals(IOUtils.toString(fileAwareInputStream.getInputStream()), "first");

    Assert.assertNull(extractor.readRecord(null));
  }

  private CopyableFile getTestCopyableFile(String resourcePath) throws IOException {
    String filePath = getClass().getClassLoader().getResource(resourcePath).getFile();
    FileStatus status = new FileStatus(0l, false, 0, 0l, 0l, new Path(filePath));

    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/publisher");

    return CopyableFile.fromOriginAndDestination(FileSystem.getLocal(new Configuration()), status,
        new Path("/destination"), CopyConfiguration.builder(FileSystem.getLocal(new Configuration()), properties)
        .preserve(PreserveAttributes.fromMnemonicString("")).build()).build();
  }
}

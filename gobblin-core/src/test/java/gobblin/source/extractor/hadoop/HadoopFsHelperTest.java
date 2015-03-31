/* (c) 2014 LinkedIn Corp. All rights reserved.
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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.source.extractor.filebased.FileBasedHelperException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.testng.Assert;
import org.testng.annotations.Test;

public class HadoopFsHelperTest {

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testConnectFailsWithS3URLWithoutAWSCredentials() throws FileBasedHelperException {
    Configuration conf = new Configuration(); // plain conf, no S3 credentials
    SourceState sourceState = new SourceState();
    sourceState.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, "s3://support.elasticmapreduce/spark/install-spark/");
    HadoopFsHelper fsHelper = new HadoopFsHelper(sourceState, conf);
    fsHelper.connect();
  }

  @Test
  public void testGetFileStreamSucceedsWithUncompressedFile() throws FileBasedHelperException, IOException {
    SourceState sourceState = new SourceState();
    URL rootUrl = getClass().getResource("/source/");
    String rootPath = rootUrl.toString();
    sourceState.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, rootPath);
    HadoopFsHelper fsHelper = new HadoopFsHelper(sourceState);

    fsHelper.connect();
    URL url = getClass().getResource("/source/simple.tsv");
    String path = url.toString();
    InputStream in = fsHelper.getFileStream(path);
    String contents = IOUtils.toString(in, "UTF-8");
    Assert.assertEquals(contents, "A\t1\nB\t2\n");
  }

  @Test
  public void testGetFileStreamSucceedsWithGZIPFile() throws FileBasedHelperException, IOException {
    SourceState sourceState = new SourceState();
    URL rootUrl = getClass().getResource("/source/");
    String rootPath = rootUrl.toString();
    sourceState.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, rootPath);
    HadoopFsHelper fsHelper = new HadoopFsHelper(sourceState);

    fsHelper.connect();
    URL url = getClass().getResource("/source/simple.tsv.gz");
    String path = url.toString();
    InputStream in = fsHelper.getFileStream(path);
    String contents = IOUtils.toString(in, "UTF-8");
    Assert.assertEquals(contents, "A\t1\nB\t2\n");
  }
}

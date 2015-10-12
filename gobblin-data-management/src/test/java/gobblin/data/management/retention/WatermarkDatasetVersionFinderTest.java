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

package gobblin.data.management.retention;

import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.data.management.retention.version.StringDatasetVersion;
import gobblin.data.management.retention.version.finder.WatermarkDatasetVersionFinder;


public class WatermarkDatasetVersionFinderTest {

  @Test
  public void testVersionParser() {
    Properties props = new Properties();

    WatermarkDatasetVersionFinder parser = new WatermarkDatasetVersionFinder(null, props);

    Assert.assertEquals(parser.versionClass(), StringDatasetVersion.class);
    Assert.assertEquals(parser.globVersionPattern(), new Path("*"));
    Assert.assertEquals(parser.getDatasetVersion(new Path("datasetVersion"), new Path("fullPath")).getVersion(),
        "datasetVersion");
    Assert.assertEquals(parser.getDatasetVersion(new Path("datasetVersion"), new Path("fullPath")).
            getPathsToDelete().iterator().next(), new Path("fullPath"));
  }

  @Test
  public void testRegex() {
    Properties props = new Properties();
    props.put(WatermarkDatasetVersionFinder.WATERMARK_REGEX_KEY, "watermark-([A-Za-z]*)-[a-z]*");

    WatermarkDatasetVersionFinder parser = new WatermarkDatasetVersionFinder(null, props);

    Assert.assertEquals(parser.versionClass(), StringDatasetVersion.class);
    Assert.assertEquals(parser.globVersionPattern(), new Path("*"));
    Assert.assertEquals(parser.getDatasetVersion(new Path("watermark-actualVersion-test"),
        new Path("fullPath")).getVersion(), "actualVersion");
  }

}

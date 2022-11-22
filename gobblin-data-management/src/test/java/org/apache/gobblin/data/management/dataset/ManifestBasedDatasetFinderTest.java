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

package org.apache.gobblin.data.management.dataset;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import org.apache.gobblin.data.management.copy.ManifestBasedDataset;
import org.apache.gobblin.data.management.copy.ManifestBasedDatasetFinder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ManifestBasedDatasetFinderTest {
  @Test
  public void testFindDataset()
      throws IOException {

    //Get manifest Path
    String manifestPath = getClass().getClassLoader().getResource("manifestBasedDistcpTest/sampleManifest.json").getPath();
    FileSystem localFs = FileSystem.getLocal(new Configuration());

    // Test manifestDatasetFinder
    Properties props = new Properties();
    props.setProperty("gobblin.copy.manifestBased.manifest.location", manifestPath);
    ManifestBasedDatasetFinder finder = new ManifestBasedDatasetFinder(localFs, props);
    List<ManifestBasedDataset> datasets = finder.findDatasets();
    Assert.assertEquals(datasets.size(), 1);
  }
}

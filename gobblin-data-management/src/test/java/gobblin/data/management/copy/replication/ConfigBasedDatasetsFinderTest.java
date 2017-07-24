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

package gobblin.data.management.copy.replication;

import gobblin.configuration.ConfigurationKeys;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import static gobblin.data.management.copy.replication.ConfigBasedDatasetsFinder.*;


/**
 * Unit test for {@link ConfigBasedDatasetsFinder}
 * @author mitu
 *
 */
@Test(groups = {"gobblin.data.management.copy.replication"})
public class ConfigBasedDatasetsFinderTest {

  @Test
  public void testGetLeafDatasetURIs() throws URISyntaxException, IOException {
    Collection<URI> allDatasetURIs = new ArrayList<URI>();
    // leaf URI
    allDatasetURIs.add(new URI("/data/derived/browsemaps/entities/anet"));
    allDatasetURIs.add(new URI("/data/derived/browsemaps/entities/comp"));
    allDatasetURIs.add(new URI("/data/derived/gowl/pymk/invitationsCreationsSends/hourly_data/aggregation/daily"));
    allDatasetURIs.add(new URI("/data/derived/gowl/pymk/invitationsCreationsSends/hourly_data/aggregation/daily_dedup"));

    // None leaf URI
    allDatasetURIs.add(new URI("/data/derived"));
    allDatasetURIs.add(new URI("/data/derived/browsemaps"));
    allDatasetURIs.add(new URI("/data/derived/browsemaps/entities/"));

    allDatasetURIs.add(new URI("/data/derived/gowl/"));
    allDatasetURIs.add(new URI("/data/derived/gowl/pymk/"));
    allDatasetURIs.add(new URI("/data/derived/gowl/pymk/invitationsCreationsSends/"));
    allDatasetURIs.add(new URI("/data/derived/gowl/pymk/invitationsCreationsSends/hourly_data/aggregation"));

    // wrong root
    allDatasetURIs.add(new URI("/data/derived2"));

    // disabled
    Set<URI> disabled = new HashSet<URI>();
    disabled.add(new URI("/data/derived/gowl/pymk/invitationsCreationsSends/hourly_data/aggregation/daily"));

    Set<URI> validURIs = ConfigBasedDatasetsFinder.getValidDatasetURIs(allDatasetURIs, disabled, new Path("/data/derived"));

    Assert.assertTrue(validURIs.size() == 3);
    Assert.assertTrue(validURIs.contains(new URI("/data/derived/gowl/pymk/invitationsCreationsSends/hourly_data/aggregation/daily_dedup")));
    Assert.assertTrue(validURIs.contains(new URI("/data/derived/browsemaps/entities/comp")));
    Assert.assertTrue(validURIs.contains(new URI("/data/derived/browsemaps/entities/anet")));
  }

  @Test
  public void testValidURIsWithBlacklist() throws URISyntaxException, IOException {
    /* tmp ConfigStore structure:
       /tmp/configStore/test/A
          /A/A1
       /tmp/configStore/test/B
          /B/B1
       /tmp/configStore/test/C
          /C/C1/C11
          /C/C1/C12

       Will finally have A1, B1, C11
     */
    FileSystem fs = FileSystem.get(new Configuration()) ;
    Properties jobProps = new Properties() ;
    fs.mkdirs(new Path("/tmp/configStore/test"));
    fs.mkdirs(new Path("/tmp/configStore/test/A"));
    fs.mkdirs(new Path("/tmp/configStore/test/B"));
    fs.mkdirs(new Path("/tmp/configStore/test/C"));
    fs.create(new Path("/tmp/configStore/test/A/A1"));
    fs.create(new Path("/tmp/configStore/test/B/B1"));
    fs.mkdirs(new Path("/tmp/configStore/test/C/C1"));
    fs.create(new Path("/tmp/configStore/test/C/C1/C11"));
    fs.create(new Path("/tmp/configStore/test/C/C1/C12"));
    jobProps.setProperty(ConfigurationKeys.CONFIG_MANAGEMENT_STORE_URI, "/tmp/configStore/");
    jobProps.setProperty(GOBBLIN_CONFIG_STORE_WHITELIST_TAG, "/");
    jobProps.setProperty(GOBBLIN_CONFIG_STORE_DATASET_COMMON_ROOT, "test");

    ConfigBasedDatasetsFinder configBasedDatasetsFinder = new ConfigBasedCopyableDatasetFinder(fs, jobProps);
    List<URI> uriList = configBasedDatasetsFinder.datasetURNtoURI("C/*");

    Assert.assertTrue(uriList.size() == 2);
    Assert.assertTrue(uriList.contains(new URI("/tmp/configStore/test/C/C1/C11")));
    Assert.assertTrue(uriList.contains(new URI("/tmp/configStore/test/C/C1/C12")));

    // Clean up
    fs.delete( new Path("/tmp/configStore"), true);
    return;
  }
}

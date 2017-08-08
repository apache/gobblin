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

package org.apache.gobblin.data.management.copy.replication;

import com.google.common.base.Optional;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import java.util.regex.Pattern;
import org.apache.hadoop.fs.Path;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


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

    Set<URI> validURIs = ConfigBasedDatasetsFinder.getValidDatasetURIsHelper(allDatasetURIs, disabled, new Path("/data/derived"));

    Assert.assertTrue(validURIs.size() == 3);
    Assert.assertTrue(validURIs.contains(new URI("/data/derived/gowl/pymk/invitationsCreationsSends/hourly_data/aggregation/daily_dedup")));
    Assert.assertTrue(validURIs.contains(new URI("/data/derived/browsemaps/entities/comp")));
    Assert.assertTrue(validURIs.contains(new URI("/data/derived/browsemaps/entities/anet")));
  }

  @Test
  public void blacklistPatternTest() {
    Properties properties = new Properties();
    properties.setProperty("gobblin.selected.policy", "random");
    properties.setProperty("source","random");
    properties.setProperty("replicas", "random");

    ConfigBasedMultiDatasets configBasedMultiDatasets = new ConfigBasedMultiDatasets();

    ReplicationConfiguration rc = Mockito.mock(ReplicationConfiguration.class);
    CopyRoute cr = Mockito.mock(CopyRoute.class);
    ConfigBasedDataset configBasedDataset = new ConfigBasedDataset(rc, new Properties(), cr, "/test/tmp/word");
    ConfigBasedDataset configBasedDataset2 = new ConfigBasedDataset(rc, new Properties(), cr, "/test/a_temporary/word");
    ConfigBasedDataset configBasedDataset3 = new ConfigBasedDataset(rc, new Properties(), cr, "/test/go/word");


    Pattern pattern1 = Pattern.compile(".*_temporary.*");
    Pattern pattern2 = Pattern.compile(".*tmp.*");
    List<Pattern> patternList = new ArrayList<>();
    patternList.add(pattern1);
    patternList.add(pattern2);

    Assert.assertFalse(configBasedMultiDatasets.blacklistFilteringHelper(configBasedDataset, Optional.of(patternList)));
    Assert.assertFalse(configBasedMultiDatasets.blacklistFilteringHelper(configBasedDataset2, Optional.of(patternList)));
    Assert.assertTrue(configBasedMultiDatasets.blacklistFilteringHelper(configBasedDataset3, Optional.of(patternList)));


  }
}

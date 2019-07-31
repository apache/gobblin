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

package org.apache.gobblin.source.extractor.extract.kafka;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

import org.apache.gobblin.config.client.ConfigClient;
import org.apache.gobblin.config.client.api.VersionStabilityPolicy;
import org.apache.gobblin.config.store.api.ConfigStoreCreationException;
import org.apache.gobblin.config.store.zip.SimpleLocalIvyConfigStoreFactory;
import org.apache.gobblin.config.store.zip.ZipFileConfigStore;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Optional;

import static org.apache.gobblin.source.extractor.extract.kafka.ConfigStoreUtils.GOBBLIN_CONFIG_COMMONPATH;
import static org.apache.gobblin.source.extractor.extract.kafka.ConfigStoreUtils.GOBBLIN_CONFIG_FILTER;
import static org.apache.gobblin.source.extractor.extract.kafka.ConfigStoreUtils.GOBBLIN_CONFIG_TAGS_WHITELIST;


/**
 * The same testing routine for ivy-based config-store (ZipConfigStore)
 * Make sure everything inside {@link ConfigStoreUtils} will work for {@link ZipFileConfigStore} implementation.
 */
public class ZipConfigStoreUtilsTest {
  private String configStoreUri;
  private ConfigClient configClient = ConfigClient.createConfigClient(VersionStabilityPolicy.WEAK_LOCAL_STABILITY);

  @BeforeClass
  public void setUp()
      throws URISyntaxException, ConfigStoreCreationException, IOException {
    Path path =
        Paths.get(ZipConfigStoreUtilsTest.class.getClassLoader().getResource("IvyConfigStoreTest.zip").getPath());
    URI zipInClassPathURI = new URI(
        "ivy-file:/?org=org&module=module&storePath=" + path
            + "&storePrefix=_CONFIG_STORE");

    ZipFileConfigStore store = new SimpleLocalIvyConfigStoreFactory().createConfigStore(zipInClassPathURI);
    configStoreUri = store.getStoreURI().toString();
  }

  @Test
  public void testGetListOfTopicNamesByFilteringTag()
      throws Exception {
    Properties properties = new Properties();
    properties.setProperty(GOBBLIN_CONFIG_TAGS_WHITELIST, "/tags/whitelist");
    properties.setProperty(GOBBLIN_CONFIG_FILTER, "/data/tracking");
    properties.setProperty(GOBBLIN_CONFIG_COMMONPATH, "/data/tracking");

    List<String> result = ConfigStoreUtils
        .getListOfTopicNamesByFilteringTag(properties, configClient, Optional.absent(), configStoreUri,
            GOBBLIN_CONFIG_TAGS_WHITELIST);
    Assert.assertEquals(result.size(), 2);
    Assert.assertTrue(result.contains("Topic1"));
    Assert.assertTrue(result.contains("Topic2"));
  }
}

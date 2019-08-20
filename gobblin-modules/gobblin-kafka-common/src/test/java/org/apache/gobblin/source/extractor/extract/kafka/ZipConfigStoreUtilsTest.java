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
import java.util.stream.Collectors;

import org.apache.gobblin.config.client.ConfigClient;
import org.apache.gobblin.config.client.api.VersionStabilityPolicy;
import org.apache.gobblin.config.store.api.ConfigStoreCreationException;
import org.apache.gobblin.config.store.zip.SimpleLocalIvyConfigStoreFactory;
import org.apache.gobblin.config.store.zip.ZipFileConfigStore;
import org.apache.gobblin.kafka.client.GobblinKafkaConsumerClient;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import static org.apache.gobblin.configuration.ConfigurationKeys.CONFIG_MANAGEMENT_STORE_ENABLED;
import static org.apache.gobblin.configuration.ConfigurationKeys.CONFIG_MANAGEMENT_STORE_URI;
import static org.apache.gobblin.source.extractor.extract.kafka.ConfigStoreUtils.GOBBLIN_CONFIG_COMMONPATH;
import static org.apache.gobblin.source.extractor.extract.kafka.ConfigStoreUtils.GOBBLIN_CONFIG_FILTER;
import static org.apache.gobblin.source.extractor.extract.kafka.ConfigStoreUtils.GOBBLIN_CONFIG_TAGS_BLACKLIST;
import static org.apache.gobblin.source.extractor.extract.kafka.ConfigStoreUtils.GOBBLIN_CONFIG_TAGS_WHITELIST;
import static org.mockito.Matchers.anyList;


/**
 * The same testing routine for ivy-based config-store (ZipConfigStore)
 * Make sure everything inside {@link ConfigStoreUtils} will work for {@link ZipFileConfigStore} implementation.
 *
 * Note that {@link ZipFileConfigStore}, doesn't contain version folder. More specifically, under .zip file
 * there would be configNodes directly, unlike {@link org.apache.gobblin.config.store.hdfs.SimpleHadoopFilesystemConfigStore}
 * where there would be a version folder inside the configStore root path.
 */
public class ZipConfigStoreUtilsTest {
  private String configStoreUri;
  private ConfigClient configClient = ConfigClient.createConfigClient(VersionStabilityPolicy.WEAK_LOCAL_STABILITY);
  private GobblinKafkaConsumerClient mockClient;

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
    mockClient = Mockito.mock(GobblinKafkaConsumerClient.class);
  }

  @Test
  public void testGetListOfTopicNamesByFilteringTag() {
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

    properties.setProperty(GOBBLIN_CONFIG_TAGS_WHITELIST, "/tags/random");
    result = ConfigStoreUtils
        .getListOfTopicNamesByFilteringTag(properties, configClient, Optional.absent(), configStoreUri,
            GOBBLIN_CONFIG_TAGS_WHITELIST);
    Assert.assertEquals(result.size(), 1);
    Assert.assertTrue(result.contains("Topic3"));

    properties.setProperty(GOBBLIN_CONFIG_TAGS_BLACKLIST, "/tags/blacklist");
    result = ConfigStoreUtils
        .getListOfTopicNamesByFilteringTag(properties, configClient, Optional.absent(), configStoreUri,
            GOBBLIN_CONFIG_TAGS_BLACKLIST);
    Assert.assertEquals(result.size(), 2);
    Assert.assertTrue(result.contains("Topic1"));
    Assert.assertTrue(result.contains("Topic2"));
  }

  @Test
  public void testGetTopicsFromConfigStore()
      throws Exception {
    KafkaTopic topic1 = new KafkaTopic("Topic1", Lists.newArrayList());
    KafkaTopic topic2 = new KafkaTopic("Topic2", Lists.newArrayList());
    KafkaTopic topic3 = new KafkaTopic("Topic3", Lists.newArrayList());

    Mockito.when(mockClient.getFilteredTopics(anyList(), anyList()))
        .thenReturn(ImmutableList.of(topic1, topic2, topic3));
    Properties properties = new Properties();

    // Empty properties returns everything: topic1, 2 and 3.
    List<KafkaTopic> result = ConfigStoreUtils.getTopicsFromConfigStore(properties, configStoreUri, mockClient);
    Assert.assertEquals(result.size(), 3);

    properties.setProperty(GOBBLIN_CONFIG_TAGS_WHITELIST, "/tags/whitelist");
    properties.setProperty(GOBBLIN_CONFIG_FILTER, "/data/tracking");
    properties.setProperty(GOBBLIN_CONFIG_COMMONPATH, "/data/tracking");

    // Whitelist only two topics. Should only returned whitelisted topics.
    result = ConfigStoreUtils.getTopicsFromConfigStore(properties, configStoreUri, mockClient);
    Assert.assertEquals(result.size(), 2);
    List<String> resultInString = result.stream().map(KafkaTopic::getName).collect(Collectors.toList());
    Assert.assertTrue(resultInString.contains("Topic1"));
    Assert.assertTrue(resultInString.contains("Topic2"));

    // Blacklist two topics. Should only return non-blacklisted topics.
    properties.remove(GOBBLIN_CONFIG_TAGS_WHITELIST);
    properties.setProperty(GOBBLIN_CONFIG_TAGS_BLACKLIST, "/tags/blacklist");
    result = ConfigStoreUtils.getTopicsFromConfigStore(properties, configStoreUri, mockClient);
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).getName(), "Topic3");
  }

  @Test
  public void testGetConfigForTopic() throws Exception {
    Properties properties = new Properties();
    String commonPath = "/data/tracking";
    properties.setProperty(GOBBLIN_CONFIG_COMMONPATH, commonPath);
    properties.setProperty(CONFIG_MANAGEMENT_STORE_URI, configStoreUri);
    properties.setProperty(CONFIG_MANAGEMENT_STORE_ENABLED, "true");
    properties.setProperty("topic.name", "Topic1");

    Config topic1Config = ConfigStoreUtils.getConfigForTopic(properties, "topic.name", configClient).get();
    Assert.assertEquals(topic1Config.getString("aaaaa"), "bbbb");
  }
}

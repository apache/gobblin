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

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import org.apache.gobblin.config.client.ConfigClient;
import org.apache.gobblin.config.client.api.VersionStabilityPolicy;
import org.apache.gobblin.kafka.client.GobblinKafkaConsumerClient;

import static org.apache.gobblin.configuration.ConfigurationKeys.CONFIG_MANAGEMENT_STORE_ENABLED;
import static org.apache.gobblin.configuration.ConfigurationKeys.CONFIG_MANAGEMENT_STORE_URI;
import static org.apache.gobblin.source.extractor.extract.kafka.ConfigStoreUtils.GOBBLIN_CONFIG_COMMONPATH;
import static org.apache.gobblin.source.extractor.extract.kafka.ConfigStoreUtils.GOBBLIN_CONFIG_FILTER;
import static org.apache.gobblin.source.extractor.extract.kafka.ConfigStoreUtils.GOBBLIN_CONFIG_TAGS_BLACKLIST;
import static org.apache.gobblin.source.extractor.extract.kafka.ConfigStoreUtils.GOBBLIN_CONFIG_TAGS_WHITELIST;
import static org.mockito.Matchers.anyList;


/**
 * Added this testing to protect no behavior changes on {@link ConfigStoreUtils} after refactoring.
 */
public class ConfigStoreUtilsTest {

  // Declare as string in convenience of testing.
  private String configStoreUri;

  private GobblinKafkaConsumerClient mockClient;

  private ConfigClient configClient = ConfigClient.createConfigClient(VersionStabilityPolicy.WEAK_LOCAL_STABILITY);

  /**
   * Loading a fs-based config-store for ease of unit testing.
   * @throws Exception
   */
  @BeforeClass
  public void setup()
      throws Exception {
    URL url = this.getClass().getClassLoader().getResource("_CONFIG_STORE");
    configStoreUri = getStoreURI(new Path(url.getPath()).getParent().toString()).toString();
    mockClient = Mockito.mock(GobblinKafkaConsumerClient.class);
  }

  @Test
  public void testGetUriStringForTopic() throws Exception {
    String commonPath = "/data/tracking";
    URI topic1URI = ConfigStoreUtils.getUriStringForTopic("Topic1", commonPath, configStoreUri);
    URI expectedTopic1URI = new URI("simple-file", "", new URI(configStoreUri).getPath() + "/data/tracking/Topic1", null, null);
    Assert.assertEquals(topic1URI, expectedTopic1URI);

    URI topic2URI = ConfigStoreUtils.getUriStringForTopic("Topic2", commonPath, configStoreUri);
    URI expectedTopic2URI = new URI("simple-file", "", new URI(configStoreUri).getPath() + "/data/tracking/Topic2", null, null);
    Assert.assertEquals(topic2URI, expectedTopic2URI);
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

  /**
   * Return localFs-based config-store uri.
   * Note that for local FS, fs.getUri will return an URI without authority. So we shouldn't add authority when
   * we construct an URI for local-file backed config-store.
   */
  private URI getStoreURI(String configDir)
      throws URISyntaxException {
    return new URI("simple-file", "", configDir, null, null);
  }
}
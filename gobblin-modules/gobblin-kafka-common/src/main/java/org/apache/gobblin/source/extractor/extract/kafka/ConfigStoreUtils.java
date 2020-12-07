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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.config.client.ConfigClient;
import org.apache.gobblin.config.client.ConfigClientUtils;
import org.apache.gobblin.config.client.api.ConfigStoreFactoryDoesNotExistsException;
import org.apache.gobblin.config.client.api.VersionStabilityPolicy;
import org.apache.gobblin.config.store.api.ConfigStoreCreationException;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.kafka.client.GobblinKafkaConsumerClient;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.DatasetFilterUtils;
import org.apache.gobblin.util.PathUtils;


@Slf4j
public class ConfigStoreUtils {
  /**
   * Used as the grouping functionality in config-store to filter certain data nodes.
   */
  public static final String GOBBLIN_CONFIG_TAGS_WHITELIST = "gobblin.config.tags.whitelist";
  public static final String GOBBLIN_CONFIG_TAGS_BLACKLIST = "gobblin.config.tags.blacklist";

  public static final String GOBBLIN_CONFIG_FILTER = "gobblin.config.filter";
  public static final String GOBBLIN_CONFIG_COMMONPATH = "gobblin.config.commonPath";

  /**
   * Will return the list of URIs given which are importing tag {@param tagUri}
   */
  public static Collection<URI> getTopicsURIFromConfigStore(ConfigClient configClient, Path tagUri, String filterString,
      Optional<Config> runtimeConfig) {
    try {
      Collection<URI> importedBy = configClient.getImportedBy(tagUri.toUri(), true, runtimeConfig);
      return importedBy.stream().filter((URI u) -> u.toString().contains(filterString)).collect(Collectors.toList());
    } catch (ConfigStoreFactoryDoesNotExistsException | ConfigStoreCreationException e) {
      throw new Error(e);
    }
  }

  public static Optional<String> getConfigStoreUri(Properties properties) {
    Optional<String> configStoreUri =
        StringUtils.isNotBlank(properties.getProperty(ConfigurationKeys.CONFIG_MANAGEMENT_STORE_URI)) ? Optional.of(
            properties.getProperty(ConfigurationKeys.CONFIG_MANAGEMENT_STORE_URI)) : Optional.<String>absent();
    if (!Boolean.valueOf(properties.getProperty(ConfigurationKeys.CONFIG_MANAGEMENT_STORE_ENABLED,
        ConfigurationKeys.DEFAULT_CONFIG_MANAGEMENT_STORE_ENABLED))) {
      configStoreUri = Optional.<String>absent();
    }
    return configStoreUri;
  }

  public static String getTopicNameFromURI(URI uri) {
    Path path = new Path(uri);
    return path.getName();
  }

  public static URI getUriStringForTopic(String topicName, String commonPath, String configStoreUri)
      throws URISyntaxException {
    Path fullTopicPathInConfigStore = PathUtils.mergePaths(new Path(commonPath), new Path(topicName));
    URI topicUri = getUriFromPath(fullTopicPathInConfigStore, configStoreUri);
    log.info("URI for topic is : " + topicUri.toString());
    return topicUri;
  }

  /**
   * Used when topic name needs to be fetched from Properties object, assuming we knew the topicKey.
   */
  public static Optional<Config> getConfigForTopic(Properties properties, String topicKey, ConfigClient configClient) {
    Preconditions.checkArgument(properties.containsKey(topicKey), "Missing required property " + topicKey);
    String topicName = properties.getProperty(topicKey);

    return getConfigForTopicWithName(properties, topicName, configClient);
  }

  /**
   * Used when topic name is known.
   */
  public static Optional<Config> getConfigForTopicWithName(Properties properties, String topicName, ConfigClient configClient) {
    Optional<String> configStoreUri = ConfigStoreUtils.getConfigStoreUri(properties);
    Optional<Config> config = Optional.<Config>absent();
    if (!configStoreUri.isPresent()) {
      return config;
    }
    try {
      Preconditions.checkArgument(properties.containsKey(GOBBLIN_CONFIG_COMMONPATH),
          "Missing required property " + GOBBLIN_CONFIG_COMMONPATH);
      String commonPath = properties.getProperty(GOBBLIN_CONFIG_COMMONPATH);
      config = Optional.fromNullable(
          ConfigStoreUtils.getConfig(configClient, ConfigStoreUtils.getUriStringForTopic(topicName, commonPath, configStoreUri.get()),
              ConfigClientUtils.getOptionalRuntimeConfig(properties)));
    } catch (URISyntaxException e) {
      log.error("Unable to get config", e);
    }
    return config;
  }

  /**
   * Wrapper to convert Checked Exception to Unchecked Exception
   * Easy to use in lambda expressions
   */
  public static Config getConfig(ConfigClient client, URI u, Optional<Config> runtimeConfig) {
    try {
      return client.getConfig(u, runtimeConfig);
    } catch (ConfigStoreFactoryDoesNotExistsException | ConfigStoreCreationException e) {
      throw new Error(e);
    }
  }

  /**
   * Get topics from config store.
   * Topics will either be whitelisted or blacklisted using tag.
   *
   * If tags are not provided, it will return all topics.
   */
  public static List<KafkaTopic> getTopicsFromConfigStore(Properties properties, String configStoreUri,
      GobblinKafkaConsumerClient kafkaConsumerClient) {
    ConfigClient configClient = ConfigClient.createConfigClient(VersionStabilityPolicy.WEAK_LOCAL_STABILITY);
    State state = new State();
    state.setProp(KafkaSource.TOPIC_WHITELIST, ".*");
    state.setProp(KafkaSource.TOPIC_BLACKLIST, StringUtils.EMPTY);
    List<KafkaTopic> allTopics =
        kafkaConsumerClient.getFilteredTopics(DatasetFilterUtils.getPatternList(state, KafkaSource.TOPIC_BLACKLIST),
            DatasetFilterUtils.getPatternList(state, KafkaSource.TOPIC_WHITELIST));
    Optional<Config> runtimeConfig = ConfigClientUtils.getOptionalRuntimeConfig(properties);

    if (properties.containsKey(GOBBLIN_CONFIG_TAGS_WHITELIST)) {
      List<String> whitelistedTopics = getListOfTopicNamesByFilteringTag(properties, configClient, runtimeConfig,
          configStoreUri, GOBBLIN_CONFIG_TAGS_WHITELIST);
      return allTopics.stream()
          .filter((KafkaTopic p) -> whitelistedTopics.contains(p.getName()))
          .collect(Collectors.toList());
    } else if (properties.containsKey(GOBBLIN_CONFIG_TAGS_BLACKLIST)) {
      List<String> blacklistedTopics = getListOfTopicNamesByFilteringTag(properties, configClient, runtimeConfig,
          configStoreUri, GOBBLIN_CONFIG_TAGS_BLACKLIST);
      return allTopics.stream()
          .filter((KafkaTopic p) -> !blacklistedTopics.contains(p.getName()))
          .collect(Collectors.toList());
    } else {
      log.warn("None of the blacklist or whitelist tags are provided");
      return allTopics;
    }
  }

  /**
   * Using the tag feature provided by Config-Store for grouping, getting a list of topics (case-sensitive,
   * need to be matched with what would be returned from kafka broker) tagged by the tag value specified
   * in job configuration.
   */
  public static List<String> getListOfTopicNamesByFilteringTag(Properties properties, ConfigClient configClient,
      Optional<Config> runtimeConfig, String configStoreUri, String tagConfName) {
    Preconditions.checkArgument(properties.containsKey(GOBBLIN_CONFIG_FILTER),
        "Missing required property " + GOBBLIN_CONFIG_FILTER);
    String filterString = properties.getProperty(GOBBLIN_CONFIG_FILTER);

    Path tagUri = new Path("/");
    try {
      tagUri = new Path(getUriFromPath(new Path(properties.getProperty(tagConfName)), configStoreUri));
    } catch (URISyntaxException ue) {
      log.error("Cannot construct a Tag URI due to the exception:", ue);
    }

    List<String> taggedTopics = new ArrayList<>();
    ConfigStoreUtils.getTopicsURIFromConfigStore(configClient, tagUri, filterString, runtimeConfig)
        .forEach(((URI u) -> taggedTopics.add(ConfigStoreUtils.getTopicNameFromURI(u))));

    return taggedTopics;
  }

  /**
   * Construct the URI for a Config-Store node given a path.
   * The implementation will be based on scheme, while the signature of this method will not be subject to
   * different implementation.
   *
   * The implementation will be different since Fs-based config-store simply append dataNode's path in the end,
   * while ivy-based config-store will require query to store those information.
   *
   * @param path The relative path of a node inside Config-Store.
   * @param configStoreUri The config store URI.
   * @return The URI to inspect a data node represented by path inside Config Store.
   * @throws URISyntaxException
   */
  private static URI getUriFromPath(Path path, String configStoreUri) throws URISyntaxException {
    URI storeUri = new URI(configStoreUri);
    return new URI(storeUri.getScheme(), storeUri.getAuthority(),
        PathUtils.mergePaths(new Path(storeUri.getPath()), path).toString(), storeUri.getQuery(), storeUri.getFragment());

  }

  /**
   * Shortlist topics from config store based on whitelist/blacklist tags and
   * add it to {@param whitelist}/{@param blacklist}
   *
   * If tags are not provided, blacklist and whitelist won't be modified
   * @deprecated Since this method contains implementation-specific way to construct TagURI inside Config-Store.
   */
  @Deprecated
  public static void setTopicsFromConfigStore(Properties properties, Set<String> blacklist, Set<String> whitelist,
      final String _blacklistTopicKey, final String _whitelistTopicKey) {
    Optional<String> configStoreUri = getConfigStoreUri(properties);
    if (!configStoreUri.isPresent()) {
      return;
    }
    ConfigClient configClient = ConfigClient.createConfigClient(VersionStabilityPolicy.WEAK_LOCAL_STABILITY);
    Optional<Config> runtimeConfig = ConfigClientUtils.getOptionalRuntimeConfig(properties);

    if (properties.containsKey(GOBBLIN_CONFIG_TAGS_WHITELIST)) {
      Preconditions.checkArgument(properties.containsKey(GOBBLIN_CONFIG_FILTER),
          "Missing required property " + GOBBLIN_CONFIG_FILTER);
      String filterString = properties.getProperty(GOBBLIN_CONFIG_FILTER);
      Path whiteListTagUri = PathUtils.mergePaths(new Path(configStoreUri.get()),
          new Path(properties.getProperty(GOBBLIN_CONFIG_TAGS_WHITELIST)));
      getTopicsURIFromConfigStore(configClient, whiteListTagUri, filterString, runtimeConfig).stream()
          .filter((URI u) -> ConfigUtils.getBoolean(getConfig(configClient, u, runtimeConfig), _whitelistTopicKey, false))
          .forEach(((URI u) -> whitelist.add(getTopicNameFromURI(u))));
    } else if (properties.containsKey(GOBBLIN_CONFIG_TAGS_BLACKLIST)) {
      Preconditions.checkArgument(properties.containsKey(GOBBLIN_CONFIG_FILTER),
          "Missing required property " + GOBBLIN_CONFIG_FILTER);
      String filterString = properties.getProperty(GOBBLIN_CONFIG_FILTER);
      Path blackListTagUri = PathUtils.mergePaths(new Path(configStoreUri.get()),
          new Path(properties.getProperty(GOBBLIN_CONFIG_TAGS_BLACKLIST)));
      getTopicsURIFromConfigStore(configClient, blackListTagUri, filterString, runtimeConfig).stream()
          .filter((URI u) -> ConfigUtils.getBoolean(getConfig(configClient, u, runtimeConfig), _blacklistTopicKey, false))
          .forEach(((URI u) -> blacklist.add(getTopicNameFromURI(u))));
    } else {
      log.warn("None of the blacklist or whitelist tags are provided");
    }
  }
}

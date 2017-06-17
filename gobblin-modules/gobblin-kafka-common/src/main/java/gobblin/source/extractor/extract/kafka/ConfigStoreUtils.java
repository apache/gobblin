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
package gobblin.source.extractor.extract.kafka;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import gobblin.config.client.ConfigClient;
import gobblin.config.client.ConfigClientUtils;
import gobblin.config.client.api.ConfigStoreFactoryDoesNotExistsException;
import gobblin.config.client.api.VersionStabilityPolicy;
import gobblin.config.store.api.ConfigStoreCreationException;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.kafka.client.GobblinKafkaConsumerClient;
import gobblin.util.ConfigUtils;
import gobblin.util.DatasetFilterUtils;
import gobblin.util.PathUtils;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;


@Slf4j
public class ConfigStoreUtils {
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
      Collection<URI> importedBy = configClient.getImportedBy(new URI(tagUri.toString()), true, runtimeConfig);
      return importedBy.stream().filter((URI u) -> u.toString().contains(filterString)).collect(Collectors.toList());
    } catch (URISyntaxException | ConfigStoreFactoryDoesNotExistsException | ConfigStoreCreationException e) {
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
    Path path =
        PathUtils.mergePaths(new Path(configStoreUri), PathUtils.mergePaths(new Path(commonPath), new Path(topicName)));
    log.info("URI for topic is : " + path.toString());
    return new URI(path.toString());
  }

  public static Optional<Config> getConfigForTopic(Properties properties, String topicKey, ConfigClient configClient) {
    Optional<String> configStoreUri = getConfigStoreUri(properties);
    Optional<Config> config = Optional.<Config>absent();
    if (!configStoreUri.isPresent()) {
      return config;
    }
    try {
      Preconditions.checkArgument(properties.containsKey(GOBBLIN_CONFIG_COMMONPATH),
          "Missing required property " + GOBBLIN_CONFIG_COMMONPATH);
      Preconditions.checkArgument(properties.containsKey(topicKey), "Missing required property " + topicKey);
      String topicName = properties.getProperty(topicKey);
      String commonPath = properties.getProperty(GOBBLIN_CONFIG_COMMONPATH);
      config = Optional.fromNullable(
          getConfig(configClient, getUriStringForTopic(topicName, commonPath, configStoreUri.get()),
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
   * After filtering out topics via tag, their config property is checked.
   * For each shortlisted topic, config must contain either property topic.blacklist or topic.whitelist
   *
   * If tags are not provided, it will return all topics
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
      Preconditions.checkArgument(properties.containsKey(GOBBLIN_CONFIG_FILTER),
          "Missing required property " + GOBBLIN_CONFIG_FILTER);
      String filterString = properties.getProperty(GOBBLIN_CONFIG_FILTER);
      Path whiteListTagUri = PathUtils.mergePaths(new Path(configStoreUri),
          new Path(properties.getProperty(GOBBLIN_CONFIG_TAGS_WHITELIST)));
      List<String> whitelistedTopics = new ArrayList<>();
      ConfigStoreUtils.getTopicsURIFromConfigStore(configClient, whiteListTagUri, filterString, runtimeConfig)
          .stream()
          .filter((URI u) -> ConfigUtils.getBoolean(ConfigStoreUtils.getConfig(configClient, u, runtimeConfig),
              KafkaSource.TOPIC_WHITELIST, false))
          .forEach(((URI u) -> whitelistedTopics.add(ConfigStoreUtils.getTopicNameFromURI(u))));

      return allTopics.stream()
          .filter((KafkaTopic p) -> whitelistedTopics.contains(p.getName()))
          .collect(Collectors.toList());
    } else if (properties.containsKey(GOBBLIN_CONFIG_TAGS_BLACKLIST)) {
      Preconditions.checkArgument(properties.containsKey(GOBBLIN_CONFIG_FILTER),
          "Missing required property " + GOBBLIN_CONFIG_FILTER);
      String filterString = properties.getProperty(GOBBLIN_CONFIG_FILTER);
      Path blackListTagUri = PathUtils.mergePaths(new Path(configStoreUri),
          new Path(properties.getProperty(GOBBLIN_CONFIG_TAGS_BLACKLIST)));
      List<String> blacklistedTopics = new ArrayList<>();
      ConfigStoreUtils.getTopicsURIFromConfigStore(configClient, blackListTagUri, filterString, runtimeConfig)
          .stream()
          .filter((URI u) -> ConfigUtils.getBoolean(ConfigStoreUtils.getConfig(configClient, u, runtimeConfig),
              KafkaSource.TOPIC_BLACKLIST, false))
          .forEach(((URI u) -> blacklistedTopics.add(ConfigStoreUtils.getTopicNameFromURI(u))));
      return allTopics.stream()
          .filter((KafkaTopic p) -> !blacklistedTopics.contains(p.getName()))
          .collect(Collectors.toList());
    } else {
      log.warn("None of the blacklist or whitelist tags are provided");
      return allTopics;
    }
  }

  /**
   * Shortlist topics from config store based on whitelist/blacklist tags and
   * add it to {@param whitelist}/{@param blacklist}
   *
   * If tags are not provided, blacklist and whitelist won't be modified
   */
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

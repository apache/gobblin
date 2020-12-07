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

package org.apache.gobblin.broker;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.broker.iface.ScopeInstance;
import org.apache.gobblin.broker.iface.ScopeType;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Used to create a default implementation of {@link org.apache.gobblin.broker.iface.SharedResourcesBroker}.
 *
 * All {@link SharedResourcesBroker}s created by this factory automatically load a set of configurations. In order of
 * preference:
 * * Programmatically supplied configurations in {@link #createDefaultTopLevelBroker(Config, ScopeInstance)}.
 * * Configurations in a broker configuration resource. The default path of the resource is {@link #DEFAULT_BROKER_CONF_FILE},
 *   but its path can be overriden with {@link #BROKER_CONF_FILE_KEY} either in programmatically supplied configurations,
 *   java properties, or environment variables.
 * * Java properties of the current JVM.
 * * Environment variables of the current shell.
 * * Hadoop configuration obtained via {@link Configuration#Configuration()}. This can be disabled setting
 *   {@link #LOAD_HADOOP_CONFIGURATION} to false.
 */
public class SharedResourcesBrokerFactory {

  public static final String LOAD_HADOOP_CONFIGURATION = BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX + ".loadHadoopConfiguration";

  public static final String BROKER_CONF_FILE_KEY = BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX + ".configuration";
  public static final String DEFAULT_BROKER_CONF_FILE = "gobblinBroker.conf";

  private static final Splitter LIST_SPLITTER = Splitter.on(",").trimResults().omitEmptyStrings();
  private static final Config BROKER_NAMESPACES_FALLBACK = ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
      .put(BrokerConstants.GOBBLIN_BROKER_CONFIG_NAMESPACES, "").build());

  /**
   * Create a root {@link SharedResourcesBroker}. Subscoped brokers should be built using
   * {@link SharedResourcesBroker#newSubscopedBuilder(ScopeInstance)}.
   *
   * In general, this method will be called only once per application, and all other brokers will Nbe children of the root
   * application broker.
   *
   * @param config The global configuration of the broker.
   * @param globalScope The scope of the root broker.
   * @param <S> The {@link ScopeType} DAG used for this broker tree.
   */
  public static <S extends ScopeType<S>> SharedResourcesBrokerImpl<S> createDefaultTopLevelBroker(Config config,
      ScopeInstance<S> globalScope) {

    if (!globalScope.getType().equals(globalScope.getType().rootScope())) {
      throw new IllegalArgumentException(String.format("The top level broker must be created at the root scope type. "
          + "%s is not a root scope type.", globalScope.getType()));
    }

    ScopeWrapper<S> scopeWrapper = new ScopeWrapper<>(globalScope.getType(), globalScope, Lists.<ScopeWrapper<S>>newArrayList());

    return new SharedResourcesBrokerImpl<>(new DefaultBrokerCache<S>(),
        scopeWrapper,
        Lists.newArrayList(new SharedResourcesBrokerImpl.ScopedConfig<>(globalScope.getType(),
            getBrokerConfig(addSystemConfigurationToConfig(config)))),
        ImmutableMap.of(globalScope.getType(), scopeWrapper));
  }

  private static InheritableThreadLocal<SharedResourcesBroker<?>> threadLocalBroker = new ThreadLocalBroker();
  private static SharedResourcesBroker<SimpleScopeType> SINGLETON;

  /**
   * Get all broker configurations from the given {@code srcConfig}. Configurations from
   * {@value BrokerConstants#GOBBLIN_BROKER_CONFIG_PREFIX} is always loaded first, then in-order from namespaces,
   * which is encoded as a comma separated string keyed by {@value BrokerConstants#GOBBLIN_BROKER_CONFIG_NAMESPACES}.
   */
  @VisibleForTesting
  static Config getBrokerConfig(Config srcConfig) {
    Config allSrcConfig = srcConfig.withFallback(BROKER_NAMESPACES_FALLBACK);
    String namespaces = allSrcConfig.getString(BrokerConstants.GOBBLIN_BROKER_CONFIG_NAMESPACES);
    Config brokerConfig = ConfigUtils.getConfigOrEmpty(allSrcConfig, BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX);

    for (String namespace : LIST_SPLITTER.splitToList(namespaces)) {
      brokerConfig = brokerConfig.withFallback(ConfigUtils.getConfigOrEmpty(allSrcConfig, namespace));
    }

    return brokerConfig;
  }

  /**
   * Get the implicit {@link SharedResourcesBroker} in the callers thread. This is either a singleton broker configured
   * from environment variables, java options, and classpath configuration options, or a specific broker injected
   * elsewhere in the application.
   *
   * In general, it is preferable to explicitly pass around {@link SharedResourcesBroker}s, as that allows better
   * control over the scoping. However, in cases where it is hard to do so, this method provides an alternative to
   * method of acquiring a configured broker.
   */
  public static SharedResourcesBroker<?> getImplicitBroker() {
    SharedResourcesBroker<?> threadLocal = threadLocalBroker.get();
    return threadLocal == null ? getSingleton() : threadLocal;
  }

  /**
   * Register a {@link SharedResourcesBroker} to be used as the implicit broker for this and all new children threads.
   */
  public static void registerImplicitBroker(SharedResourcesBroker<?> broker) {
    threadLocalBroker.set(broker);
  }

  private static synchronized SharedResourcesBroker<SimpleScopeType> getSingleton() {
    if (SINGLETON == null) {
      SINGLETON = createDefaultTopLevelBroker(ConfigFactory.empty(), SimpleScopeType.GLOBAL.defaultScopeInstance());
    }
    return SINGLETON;
  }

  private static class ThreadLocalBroker extends InheritableThreadLocal<SharedResourcesBroker<?>> {}

  private static Config addSystemConfigurationToConfig(Config config) {
    Map<String, String> confMap = Maps.newHashMap();
    addBrokerKeys(confMap, System.getenv().entrySet());
    addBrokerKeys(confMap, System.getProperties().entrySet());

    Config systemConfig = ConfigFactory.parseMap(confMap);

    Config tmpConfig = config.withFallback(systemConfig);
    String brokerConfPath = DEFAULT_BROKER_CONF_FILE;
    if (tmpConfig.hasPath(BROKER_CONF_FILE_KEY)) {
      brokerConfPath = tmpConfig.getString(BROKER_CONF_FILE_KEY);
    }

    Config resourceConfig = ConfigFactory.parseResources(SharedResourcesBrokerFactory.class, brokerConfPath);

    config = config.withFallback(resourceConfig).withFallback(systemConfig);

    if (ConfigUtils.getBoolean(config, LOAD_HADOOP_CONFIGURATION, true)) {
      Map<String, String> hadoopConfMap = Maps.newHashMap();
      Configuration hadoopConf = new Configuration();
      hadoopConf.addResource("gobblin-site.xml");
      addBrokerKeys(hadoopConfMap, hadoopConf);
      config = config.withFallback(ConfigFactory.parseMap(hadoopConfMap));
    }

    return config;
  }

  public static <S, T> void addBrokerKeys(Map<String, String> configMap, Iterable<Map.Entry<S, T>> entries) {
    for (Map.Entry<S, T> entry : entries) {
      Object key = entry.getKey();
      if (key instanceof String && ((String) key).startsWith(BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX)) {
        configMap.put((String) key, entry.getValue().toString());
      }
    }
  }

}

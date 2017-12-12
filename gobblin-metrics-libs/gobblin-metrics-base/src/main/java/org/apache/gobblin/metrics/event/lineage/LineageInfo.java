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

package org.apache.gobblin.metrics.event.lineage;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.broker.EmptyKey;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.iface.NotConfiguredException;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.dataset.DatasetDescriptor;
import org.apache.gobblin.dataset.DatasetResolver;
import org.apache.gobblin.dataset.DatasetResolverFactory;
import org.apache.gobblin.dataset.NoopDatasetResolver;
import org.apache.gobblin.metrics.broker.LineageInfoFactory;
import org.apache.gobblin.util.ConfigUtils;


/**
 * The lineage coordinator in a Gobblin job with single source and multiple destinations
 *
 * <p>
 *   In Gobblin, a work unit processes records from only one dataset. It writes output to one or more destinations,
 *   depending on the number of branches configured in the job. One destination means an output as another dataset.
 * </p>
 *
 * <p>
 *   Lineage info is jointly collected from the source, represented by {@link org.apache.gobblin.source.Source} or
 *   {@link org.apache.gobblin.source.extractor.Extractor}, and destination,
 *   represented by {@link org.apache.gobblin.writer.DataWriter} or {@link org.apache.gobblin.publisher.DataPublisher}
 * </p>
 *
 * <p>
 *   The general flow is:
 *   <ol>
 *     <li> get a {@link LineageInfo} instance with {@link LineageInfo#getLineageInfo(SharedResourcesBroker)}</li>
 *     <li> source sets its {@link DatasetDescriptor} to each work unit </li>
 *     <li> destination puts its {@link DatasetDescriptor} to the work unit </li>
 *     <li> load and send all lineage events from all states </li>
 *     <li> purge lineage info from all states </li>
 *   </ol>
 * </p>
 */
@Slf4j
public final class LineageInfo {
  private static final String DATASET_RESOLVER_FACTORY = "datasetResolverFactory";
  private static final String DATASET_RESOLVER_CONFIG_NAMESPACE = "datasetResolver";

  private static final String BRANCH = "branch";
  private static final Gson GSON = new Gson();
  private static final String NAME_KEY = "name";

  private static final Config FALLBACK =
      ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
          .put(DATASET_RESOLVER_FACTORY, NoopDatasetResolver.FACTORY)
          .build());

  private final DatasetResolver resolver;

  public LineageInfo(Config config) {
    resolver = getResolver(config.withFallback(FALLBACK));
  }

  /**
   * Set source {@link DatasetDescriptor} of a lineage event
   *
   * <p>
   *   Only the {@link org.apache.gobblin.source.Source} or its {@link org.apache.gobblin.source.extractor.Extractor}
   *   is supposed to set the source for a work unit of a dataset
   * </p>
   *
   * @param state state about a {@link org.apache.gobblin.source.workunit.WorkUnit}
   *
   */
  public void setSource(DatasetDescriptor source, State state) {
    DatasetDescriptor descriptor = resolver.resolve(source, state);
    if (descriptor == null) {
      return;
    }

    state.setProp(getKey(NAME_KEY), descriptor.getName());
    state.setProp(getKey(LineageEventBuilder.SOURCE), GSON.toJson(descriptor));
  }

  /**
   * Put a {@link DatasetDescriptor} of a destination dataset to a state
   *
   * <p>
   *   Only the {@link org.apache.gobblin.writer.DataWriter} or {@link org.apache.gobblin.publisher.DataPublisher}
   *   is supposed to put the destination dataset information. Since different branches may concurrently put,
   *   the method is implemented to be threadsafe
   * </p>
   */
  public void putDestination(DatasetDescriptor destination, int branchId, State state) {
    if (!hasLineageInfo(state)) {
      log.warn("State has no lineage info but branch " + branchId + " puts a destination: " + GSON.toJson(destination));
      return;
    }
    log.debug(String.format("Put destination %s for branch %d", GSON.toJson(destination), branchId));
    synchronized (state.getProp(getKey(NAME_KEY))) {
      DatasetDescriptor descriptor = resolver.resolve(destination, state);
      if (descriptor == null) {
        return;
      }

      state.setProp(getKey(BRANCH, branchId, LineageEventBuilder.DESTINATION), GSON.toJson(descriptor));
    }
  }

  /**
   * Load all lineage information from {@link State}s of a dataset
   *
   * @param states All states which belong to the same dataset
   * @return A collection of {@link LineageEventBuilder}s put in the state
   */
  public static Collection<LineageEventBuilder> load(Collection<? extends State> states) {
    Preconditions.checkArgument(states != null && !states.isEmpty());
    Set<LineageEventBuilder> allEvents = Sets.newHashSet();
    for (State state : states) {
      Map<String, LineageEventBuilder> branchedEvents = load(state);
      allEvents.addAll(branchedEvents.values());
    }
    return allEvents;
  }

  /**
   * Load all lineage info from a {@link State}
   *
   * @return A map from branch to its lineage info. If there is no destination info, return an empty map
   */
  static Map<String, LineageEventBuilder> load(State state) {
    String name = state.getProp(getKey(NAME_KEY));
    DatasetDescriptor source = GSON.fromJson(state.getProp(getKey(LineageEventBuilder.SOURCE)), DatasetDescriptor.class);

    String branchedPrefix = getKey(BRANCH, "");
    Map<String, LineageEventBuilder> events = Maps.newHashMap();
    for (Map.Entry<Object, Object> entry : state.getProperties().entrySet()) {
      String key = entry.getKey().toString();
      if (!key.startsWith(branchedPrefix)) {
        continue;
      }

      String[] parts = key.substring(branchedPrefix.length()).split("\\.");
      assert parts.length == 2;
      String branchId = parts[0];
      LineageEventBuilder event = events.get(branchId);
      if (event == null) {
        event = new LineageEventBuilder(name);
        event.setSource(new DatasetDescriptor(source));
        events.put(parts[0], event);
      }
      switch (parts[1]) {
        case LineageEventBuilder.DESTINATION:
          DatasetDescriptor destination = GSON.fromJson(entry.getValue().toString(), DatasetDescriptor.class);
          event.setDestination(destination);
          break;
        default:
          throw new RuntimeException("Unsupported lineage key: " + key);
      }
    }

    return events;
  }

  /**
   * Remove all lineage related properties from a state
   */
  public static void purgeLineageInfo(State state) {
    state.removePropsWithPrefix(LineageEventBuilder.LIENAGE_EVENT_NAMESPACE);
  }

  /**
   * Check if the given state has lineage info
   */
  public static boolean hasLineageInfo(State state) {
    return state.contains(getKey(NAME_KEY));
  }

  /**
   * Get the full lineage event name from a state
   */
  public static String getFullEventName(State state) {
    return Joiner.on('.').join(LineageEventBuilder.LIENAGE_EVENT_NAMESPACE, state.getProp(getKey(NAME_KEY)));
  }


  /**
   * Try to get a {@link LineageInfo} instance from the given {@link SharedResourcesBroker}
   */
  public static Optional<LineageInfo> getLineageInfo(@Nullable SharedResourcesBroker<GobblinScopeTypes> broker) {
    if (broker == null) {
      log.warn("Null broker. Will not track data lineage");
      return Optional.absent();
    }

    try {
      LineageInfo lineageInfo = broker.getSharedResource(new LineageInfoFactory(), EmptyKey.INSTANCE);
      return Optional.of(lineageInfo);
    } catch (NotConfiguredException e) {
      log.warn("Fail to get LineageInfo instance from broker. Will not track data lineage", e);
      return Optional.absent();
    }
  }

  /**
   * Get the configured {@link DatasetResolver} from {@link State}
   */
  public static DatasetResolver getResolver(Config config) {
    String resolverFactory = config.getString(DATASET_RESOLVER_FACTORY);
    if (resolverFactory.equals(NoopDatasetResolver.FACTORY)) {
      return NoopDatasetResolver.INSTANCE;
    }

    DatasetResolver resolver = NoopDatasetResolver.INSTANCE;
    try {
      DatasetResolverFactory factory = (DatasetResolverFactory) Class.forName(resolverFactory).newInstance();
      resolver = factory.createResolver(ConfigUtils.getConfigOrEmpty(config, DATASET_RESOLVER_CONFIG_NAMESPACE));
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      log.error(String.format("Fail to create a DatasetResolver with factory class %s", resolverFactory));
    }

    return resolver;
  }

  /**
   * Prefix all keys with {@link LineageEventBuilder#LIENAGE_EVENT_NAMESPACE}
   */
  private static String getKey(Object... objects) {
    Object[] args = new Object[objects.length + 1];
    args[0] = LineageEventBuilder.LIENAGE_EVENT_NAMESPACE;
    System.arraycopy(objects, 0, args, 1, objects.length);
    return LineageEventBuilder.getKey(args);
  }
}

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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.dataset.DatasetDescriptor;


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
 *     <li> source sets its {@link DatasetDescriptor} to each work unit </li>
 *     <li> destination puts its {@link DatasetDescriptor} to the work unit </li>
 *     <li> load and send all lineage events from all states </li>
 *     <li> purge lineage info from all states </li>
 *   </ol>
 * </p>
 */
@Slf4j
public final class LineageInfo {
  public static final String BRANCH = "branch";

  private static final Gson GSON = new Gson();
  private static final String NAME_KEY = "name";

  private LineageInfo() {
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
  public static void setSource(DatasetDescriptor source, State state) {
    state.setProp(getKey(NAME_KEY), source.getName());
    state.setProp(getKey(LineageEventBuilder.SOURCE), GSON.toJson(source));
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
  public static void putDestination(DatasetDescriptor destination, int branchId, State state) {
    if (!hasLineageInfo(state)) {
      log.warn("State has no lineage info but branch " + branchId + " puts a destination: " + GSON.toJson(destination));
      return;
    }

    synchronized (state.getProp(getKey(NAME_KEY))) {
      state.setProp(getKey(BRANCH, branchId, LineageEventBuilder.DESTINATION), GSON.toJson(destination));
    }
  }

  /**
   * Load all lineage information from {@link State}s of a dataset
   *
   * <p>
   *   For a dataset, the same branch across different {@link State}s must be the same, as
   *   the same branch means the same destination
   * </p>
   *
   * @param states All states which belong to the same dataset
   * @return A collection of {@link LineageEventBuilder}s put in the state
   * @throws LineageException.ConflictException if two states have conflict lineage info
   */
  public static Collection<LineageEventBuilder> load(Collection<? extends State> states)
      throws LineageException {
    Preconditions.checkArgument(states != null && !states.isEmpty());
    final Map<String, LineageEventBuilder> resultEvents = Maps.newHashMap();
    for (State state : states) {
      Map<String, LineageEventBuilder> branchedEvents = load(state);
      for (Map.Entry<String, LineageEventBuilder> entry : branchedEvents.entrySet()) {
        String branch = entry.getKey();
        LineageEventBuilder event = entry.getValue();
        LineageEventBuilder resultEvent = resultEvents.get(branch);
        if (resultEvent == null) {
          resultEvents.put(branch, event);
        } else if (!resultEvent.equals(event)) {
          throw new LineageException.ConflictException(branch, event, resultEvent);
        }
      }
    }
    return resultEvents.values();
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
          destination.addMetadata(BRANCH, branchId);
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
   * Prefix all keys with {@link LineageEventBuilder#LIENAGE_EVENT_NAMESPACE}
   */
  private static String getKey(Object... objects) {
    Object[] args = new Object[objects.length + 1];
    args[0] = LineageEventBuilder.LIENAGE_EVENT_NAMESPACE;
    System.arraycopy(objects, 0, args, 1, objects.length);
    return LineageEventBuilder.getKey(args);
  }
}

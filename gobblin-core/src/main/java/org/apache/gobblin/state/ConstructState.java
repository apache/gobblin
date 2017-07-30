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

package gobblin.state;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.Properties;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import gobblin.Constructs;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;


/**
 * Contains the state of a Gobblin construct at the end of a task. It can be merged with the {@link WorkUnitState},
 * allowing constructs to mutate the {@link WorkUnitState} (for example for a changed effective watermark), or add
 * values to report as metadata of task success/failure events.
 */
public class ConstructState extends State {

  private static final Gson GSON = new Gson();
  private static Type TYPE_OF_HASHMAP = new TypeToken<Map<String, String>>() { }.getType();
  private static final String OVERWRITE_PROPS_KEY = "gobblin.util.final.state.overwrite.props";
  private static final String FINAL_CONSTRUCT_STATE_PREFIX = "construct.final.state.";

  public ConstructState() {
  }

  public ConstructState(Properties properties) {
    super(properties);
  }

  public ConstructState(State otherState) {
    super(otherState);
  }

  /**
   * Add a set of properties that will overwrite properties in the {@link WorkUnitState}.
   * @param properties Properties to override.
   */
  public void addOverwriteProperties(Map<String, String> properties) {
    Map<String, String> previousOverwriteProps = getOverwritePropertiesMap();
    previousOverwriteProps.putAll(properties);
    setProp(OVERWRITE_PROPS_KEY, serializeMap(previousOverwriteProps));
  }

  /**
   * Add a set of properties that will overwrite properties in the {@link WorkUnitState}.
   * @param state Properties to override.
   */
  public void addOverwriteProperties(State state) {
    Map<String, String> propsMap = Maps.newHashMap();
    for (String key : state.getPropertyNames()) {
      propsMap.put(key, state.getProp(key));
    }
    addOverwriteProperties(propsMap);
  }

  /**
   * See {@link #addConstructState(Constructs, ConstructState, Optional)}. This method uses no infix.
   */
  public void addConstructState(Constructs construct, ConstructState constructState) {
    addConstructState(construct, constructState, Optional.<String>absent());
  }

  /**
   * See {@link #addConstructState(Constructs, ConstructState, Optional)}. This is a convenience method to pass a
   * String infix.
   */
  public void addConstructState(Constructs construct, ConstructState constructState, String infix) {
    addConstructState(construct, constructState, Optional.of(infix));
  }

  /**
   * Merge a {@link ConstructState} for a child construct into this {@link ConstructState}.
   *
   * <p>
   *   Non-override property names will be mutated as follows: key -> construct.name() + infix + key
   * </p>
   *
   * @param construct type of the child construct.
   * @param constructState {@link ConstructState} to merge.
   * @param infix infix added to each non-override key (for example converter number if there are multiple converters).
   */
  public void addConstructState(Constructs construct, ConstructState constructState, Optional<String> infix) {
    addOverwriteProperties(constructState.getOverwritePropertiesMap());
    constructState.removeProp(OVERWRITE_PROPS_KEY);

    for (String key : constructState.getPropertyNames()) {
      setProp(construct.name() + "." + (infix.isPresent() ? infix.get() + "." : "") + key, constructState.getProp(key));
    }

    addAll(constructState);
  }

  /**
   * Merge this {@link ConstructState} into a {@link WorkUnitState}. All override properties will be added as-is to the
   * {@lik WorkUnitState}, and possibly override already present properties. All other properties have their keys
   * mutated key -> {@link #FINAL_CONSTRUCT_STATE_PREFIX} + key, and added to the {@link WorkUnitState}.
   */
  public void mergeIntoWorkUnitState(WorkUnitState state) {
    Properties overwriteProperties = getOverwriteProperties();
    state.addAll(overwriteProperties);
    removeProp(OVERWRITE_PROPS_KEY);

    for (String key : getPropertyNames()) {
      state.setProp(FINAL_CONSTRUCT_STATE_PREFIX + key, getProp(key));
    }

  }

  /**
   * @return a {@link Map} of all override properties.
   */
  public Map<String, String> getOverwritePropertiesMap() {

    return contains(OVERWRITE_PROPS_KEY) ?
        deserializeMap(getProp(OVERWRITE_PROPS_KEY)) :
        Maps.<String, String>newHashMap();
  }

  /**
   * @return a {@link Properties} object of all override properties.
   */
  public Properties getOverwriteProperties() {
    Properties props = new Properties();
    props.putAll(getOverwritePropertiesMap());
    return props;
  }

  private static String serializeMap(Map<String, String> map) {
    return GSON.toJson(map);
  }

  private static Map<String, String> deserializeMap(String string) {
    return GSON.fromJson(string, TYPE_OF_HASHMAP);
  }

}

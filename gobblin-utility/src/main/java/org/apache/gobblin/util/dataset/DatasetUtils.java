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

package org.apache.gobblin.util.dataset;

import java.util.Map;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.StateUtils;
import org.apache.gobblin.source.workunit.WorkUnit;


public class DatasetUtils {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetUtils.class);

  public static final String DATASET = "dataset";

  /**
   * A configuration key that allows a user to specify config parameters on a dataset specific level. The value of this
   * config should be a JSON array. Each entry should be a {@link JsonObject} and should contain a
   * {@link com.google.gson.JsonPrimitive} that identifies the dataset. All configs in each dataset entry will
   * be added to the {@link WorkUnit}s for that dataset.
   *
   * <p>
   *   An example value could be: "[{"dataset" : "myDataset1", "writer.partition.columns" : "header.memberId"},
   *   {"dataset" : "myDataset2", "writer.partition.columns" : "auditHeader.time"}]".
   * </p>
   *
   * <p>
   *   The "dataset" field also allows regular expressions. For example, one can specify key, value
   *   "dataset" : "myDataset.*". In this case all datasets whose name matches the pattern "myDataset.*" will have
   *   all the specified config properties added to their {@link WorkUnit}s. If more a dataset matches multiple
   *   "dataset"s then the properties from all the {@link JsonObject}s will be added to their {@link WorkUnit}s.
   * </p>
   */
  public static final String DATASET_SPECIFIC_PROPS = DATASET + ".specific.props";

  /**
   * For backward compatibility.
   */
  private static final String KAFKA_TOPIC_SPECIFIC_STATE = "kafka.topic.specific.state";

  private DatasetUtils() {}

  /**
   * Given a {@link Iterable} of dataset identifiers (e.g., name, URN, etc.), return a {@link Map} that links each
   * dataset with the extra configuration information specified in the state via {@link #DATASET_SPECIFIC_PROPS}.
   */
  public static Map<String, State> getDatasetSpecificProps(Iterable<String> datasets, State state) {
    if (!Strings.isNullOrEmpty(state.getProp(DATASET_SPECIFIC_PROPS))
        || !Strings.isNullOrEmpty(state.getProp(KAFKA_TOPIC_SPECIFIC_STATE))) {
      Map<String, State> datasetSpecificConfigMap = Maps.newHashMap();

      JsonArray array = !Strings.isNullOrEmpty(state.getProp(DATASET_SPECIFIC_PROPS))
          ? state.getPropAsJsonArray(DATASET_SPECIFIC_PROPS) : state.getPropAsJsonArray(KAFKA_TOPIC_SPECIFIC_STATE);

      // Iterate over the entire JsonArray specified by the config key
      for (JsonElement datasetElement : array) {

        // Check that each entry in the JsonArray is a JsonObject
        Preconditions.checkArgument(datasetElement.isJsonObject(),
            "The value for property " + DATASET_SPECIFIC_PROPS + " is malformed");
        JsonObject object = datasetElement.getAsJsonObject();

        // Only process JsonObjects that have a dataset identifier
        if (object.has(DATASET)) {
          JsonElement datasetNameElement = object.get(DATASET);
          Preconditions.checkArgument(datasetNameElement.isJsonPrimitive(), "The value for property "
              + DATASET_SPECIFIC_PROPS + " is malformed, the " + DATASET + " field must be a string");

          // Iterate through each dataset that matches the value of the JsonObjects DATASET field
          for (String dataset : Iterables.filter(datasets, new DatasetPredicate(datasetNameElement.getAsString()))) {

            // If an entry already exists for a dataset, add it to the current state, else create a new state
            if (datasetSpecificConfigMap.containsKey(dataset)) {
              datasetSpecificConfigMap.get(dataset).addAll(StateUtils.jsonObjectToState(object, DATASET));
            } else {
              datasetSpecificConfigMap.put(dataset, StateUtils.jsonObjectToState(object, DATASET));
            }
          }
        } else {
          LOG.warn("Skipping JsonElement " + datasetElement + " as it is does not contain a field with key " + DATASET);
        }
      }
      return datasetSpecificConfigMap;
    }
    return Maps.newHashMap();
  }

  /**
   * Implementation of {@link Predicate} that takes in a dataset regex via its constructor. It returns true in the
   * {@link #apply(String)} method only if the dataset regex matches the specified dataset identifier.
   */
  private static class DatasetPredicate implements Predicate<String> {

    private final Pattern datasetPattern;

    private DatasetPredicate(String datasetRegex) {
      this.datasetPattern = Pattern.compile(datasetRegex, Pattern.CASE_INSENSITIVE);
    }

    @Override
    public boolean apply(String input) {
      return this.datasetPattern.matcher(input).matches();
    }
  }
}

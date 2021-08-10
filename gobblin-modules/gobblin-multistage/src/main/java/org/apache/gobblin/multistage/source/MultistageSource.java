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

package org.apache.gobblin.multistage.source;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.multistage.configuration.MultistageProperties;
import org.apache.gobblin.multistage.extractor.MultistageExtractor;
import org.apache.gobblin.multistage.factory.SchemaReaderFactory;
import org.apache.gobblin.multistage.keys.JobKeys;
import org.apache.gobblin.multistage.util.EndecoUtils;
import org.apache.gobblin.multistage.util.HdfsReader;
import org.apache.gobblin.multistage.util.JsonSchema;
import org.apache.gobblin.multistage.util.JsonUtils;
import org.apache.gobblin.multistage.util.WatermarkDefinition;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.WatermarkInterval;
import org.apache.gobblin.source.extractor.extract.AbstractSource;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.joda.time.DateTime;
import org.testng.Assert;


/**
 * This is the base Source class of multi-stage connectors.
 *
 * MultistageSource, like other Gobblin Source classes, is responsible
 * for planning. Specifically MultistageSource has following functions:
 *
 * 1. Generate work units when called by Gobblin Framework
 * 2. Instantiate Extractors
 *
 * Gobblin first instantiate a MultistageSource from one of its sub-classes,
 * then calls the getWorkUnits() method. The input to getWorkUnits() is SourceState.
 *
 * After getting a list of work units, Gobblin will instantiate again one
 * MultistageSource from one of its sub-classes for each of the work unit,
 * and then call the getExtractor() method on each instance. The input to
 * getExtractor() is WorkUnitState.
 *
 * @author chrli
 * @param <S> The schema class
 * @param <D> The data class
 */

@Slf4j
@SuppressWarnings("unchecked")
public class MultistageSource<S, D> extends AbstractSource<S, D> {
  final static private Gson GSON = new Gson();
  final static private String PROPERTY_SEPARATOR = ".";
  final static private String DUMMY_DATETIME_WATERMARK_START = "2019-01-01";
  final static private String CURRENT_DATE_SYMBOL = "-";
  final static private String ACTIVATION_WATERMARK_NAME = "activation";
  final private static String KEY_WORD_ACTIVATION = "activation";
  final private static String KEY_WORD_AUTHENTICATION = "authentication";
  final private static String KEY_WORD_COLUMN_NAME = "columnName";
  final private static String KEY_WORD_ITEMS = "items";
  final private static String KEY_WORD_SNAPSHOT_ONLY = "SNAPSHOT_ONLY";
  // Avoid too many partition created from misconfiguration, Months * Days * Hours
  final private static int MAX_DATETIME_PARTITION = 3 * 30 * 24;

  @Getter(AccessLevel.PUBLIC)
  @Setter(AccessLevel.MODULE)
  protected SourceState sourceState = null;
  @Getter(AccessLevel.PUBLIC)
  @Setter(AccessLevel.PUBLIC)
  protected JobKeys jobKeys = new JobKeys();
  @Getter(AccessLevel.PUBLIC)
  @Setter(AccessLevel.MODULE)
  SchemaReaderFactory schemaReaderFactory;

  final private ConcurrentHashMap<MultistageExtractor<S, D>, WorkUnitState> extractorState =
      new ConcurrentHashMap<>();

  protected void initialize(State state) {
    jobKeys.initialize(state);
    createSchemaReaderFactory(state);
    String urn  = MultistageProperties.MSTAGE_SOURCE_SCHEMA_URN.getValidNonblankWithDefault(state);
    jobKeys.setOutputSchema(getOutputSchema(state, urn));
  }

  /**
   * getWorkUnits() is the first place to receive the Source State object, therefore
   * initialization of parameters cannot be complete in constructor.
   */
  @SneakyThrows
  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    sourceState = state;
    initialize(state);

    if (!jobKeys.validate(state)) {
      return new ArrayList<>();
    }

    // Parse watermark settings if defined
    List<WatermarkDefinition> definedWatermarks = Lists.newArrayList();
    for (JsonElement definitionJson : jobKeys.getWatermarkDefinition()) {
      Assert.assertTrue(definitionJson.isJsonObject());
      definedWatermarks.add(new WatermarkDefinition(
          definitionJson.getAsJsonObject(), jobKeys.getIsPartialPartition(),
          jobKeys.getWorkUnitPartitionType()));
    }

    Map<String, JsonArray> secondaryInputs = readSecondaryInputs(sourceState, jobKeys.getRetryCount());
    JsonArray activations = secondaryInputs.get(KEY_WORD_ACTIVATION);
    JsonArray authentications = secondaryInputs.get(KEY_WORD_AUTHENTICATION);

    if (activations != null && activations.size() > 0) {
      definedWatermarks.add(new WatermarkDefinition(ACTIVATION_WATERMARK_NAME, activations));
    }

    // get previous high watermarks by each watermark partition or partition combinations
    // if there are multiple partitioned watermarks, such as one partitioned datetime watermark
    // and one partitioned activation (unit) watermark
    Map<String, Long> previousHighWatermarks = getPreviousHighWatermarks();
    state.setProp(ConfigurationKeys.EXTRACT_IS_FULL_KEY, checkFullExtractState(state, previousHighWatermarks));

    // generated work units based on watermarks defined and previous high watermarks
    List<WorkUnit> wuList = generateWorkUnits(definedWatermarks, previousHighWatermarks);
    if (authentications != null && authentications.size() == 1) {
      for (WorkUnit wu : wuList) {
        wu.setProp(MultistageProperties.MSTAGE_ACTIVATION_PROPERTY.toString(),
            getUpdatedWorkUnitActivation(wu, authentications.get(0).getAsJsonObject()));
      }
    }
    return wuList;
  }

  /**
   * reads the multistage source to get the secondary input categories - authentication and activation
   * In case the token is missing, it will retry accessing the tokens as per the retry parameters
   * ("delayInSec", "retryCount")
   */
  private Map<String, JsonArray> readSecondaryInputs(State state, final long retries)
      throws InterruptedException {
    log.info("Trying to read secondary input with retry = {}", retries);
    Map<String, JsonArray> secondaryInputs = readContext(state);

    // Check if authentication is ready, and if not, whether retry is required
    JsonArray authentications = secondaryInputs.get(KEY_WORD_AUTHENTICATION);
    if ((authentications == null || authentications.size() == 0)
        && jobKeys.getIsSecondaryAuthenticationEnabled() && retries > 0) {
      log.info("Authentication tokens are expected from secondary input, but not ready");
      log.info("Will wait for {} seconds and then retry reading the secondary input", jobKeys.getRetryDelayInSec());
      TimeUnit.SECONDS.sleep(jobKeys.getRetryDelayInSec());
      return readSecondaryInputs(state, retries - 1);
    }
    log.info("Successfully read secondary input, no more retry");
    return secondaryInputs;
  }

  /**
   * Default multi-stage source behavior, each protocol shall override this with more concrete function
   * @param state WorkUnitState passed in from Gobblin framework
   * @return an MultistageExtractor instance
   */
  @Override
  public Extractor<S, D> getExtractor(WorkUnitState state) {
    try {
      ClassLoader loader = this.getClass().getClassLoader();
      Class extractorClass = loader.loadClass(MultistageProperties.MSTAGE_EXTRACTOR_CLASS.getValidNonblankWithDefault(state));
      Constructor<MultistageExtractor<?, ?>> constructor = extractorClass.getConstructor(WorkUnitState.class, JobKeys.class);
      MultistageExtractor<S, D> extractor = (MultistageExtractor<S, D>) constructor.newInstance(state, this.jobKeys);
      extractorState.put(extractor, state);
      extractor.setConnection(null);
      return extractor;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * provide a default implementation
   * @param state Source State
   */
  @Override
  public void shutdown(SourceState state) {
    log.info("MultistageSource Shutdown() called, instructing extractors to close connections");
    for (MultistageExtractor<S, D> extractor: extractorState.keySet()) {
      extractor.closeConnection();
    }
  }

  List<WorkUnit> generateWorkUnits(List<WatermarkDefinition> definitions, Map<String, Long> previousHighWatermarks) {
    Assert.assertNotNull(sourceState);
    WatermarkDefinition datetimeWatermark = null;
    WatermarkDefinition unitWatermark = null;

    for (WatermarkDefinition wmd: definitions) {
      if (wmd.getType() == WatermarkDefinition.WatermarkTypes.DATETIME) {
        if (datetimeWatermark != null) {
          throw new RuntimeException("1 and only datetime type watermark is allowed.");
        }
        datetimeWatermark = wmd;
      }
      if (wmd.getType() == WatermarkDefinition.WatermarkTypes.UNIT) {
        if (unitWatermark != null) {
          throw new RuntimeException("1 and only unit type watermark is allowed"
              + ", including the unit watermark generated from secondary input.");
        }
        unitWatermark = wmd;
      }
    }
    // Set default unit watermark
    if (unitWatermark == null) {
      JsonArray unitArray = new JsonArray();
      unitArray.add(new JsonObject());
      unitWatermark = new WatermarkDefinition("unit", unitArray);
    }
    // Set default datetime watermark
    if (datetimeWatermark == null) {
      datetimeWatermark = new WatermarkDefinition("datetime", DUMMY_DATETIME_WATERMARK_START, CURRENT_DATE_SYMBOL);
    }

    List<WorkUnit> workUnits = new ArrayList<>();
    Extract extract = createExtractObject(checkFullExtractState(sourceState, previousHighWatermarks));
    List<ImmutablePair<Long, Long>> datetimePartitions = getDatetimePartitions(datetimeWatermark.getRangeInDateTime());
    List<String> unitPartitions = unitWatermark.getUnits();

    JsonArray watermarkGroups = new JsonArray();
    String datetimeWatermarkName = datetimeWatermark.getLongName();
    String unitWatermarkName = unitWatermark.getLongName();
    watermarkGroups.add(datetimeWatermarkName);
    watermarkGroups.add(unitWatermarkName);

    // only create work unit when the high range is greater than cutoff time
    // cutoff time is moved backward by GRACE_PERIOD
    // cutoff time is moved forward by ABSTINENT_PERIOD
    Long cutoffTime = previousHighWatermarks.size() == 0 ? -1 : Collections.max(previousHighWatermarks.values())
        - MultistageProperties.MSTAGE_GRACE_PERIOD_DAYS.getMillis(sourceState);
    log.debug("Overall cutoff time: {}", cutoffTime);

    for (ImmutablePair<Long, Long> dtPartition : datetimePartitions) {
      log.debug("dtPartition: {}", dtPartition);
      for (String unitPartition: unitPartitions) {

        // adding the date time partition and unit partition combination to work units until
        // it reaches ms.work.unit.parallelism.max. a combination is not added if its prior
        // watermark doesn't require a rerun.
        // a work unit signature is a date time partition and unit partition combination.
        if (MultistageProperties.MSTAGE_WORK_UNIT_PARALLELISM_MAX.validateNonblank(sourceState)
            && workUnits.size() >= (Integer) MultistageProperties.MSTAGE_WORK_UNIT_PARALLELISM_MAX.getProp(sourceState)) {
          break;
        }

        // each work unit has a watermark since we use dataset.urn to track state
        // and the work unit can be uniquely identified by its signature
        String wuSignature = getWorkUnitSignature(
            datetimeWatermarkName, dtPartition.getLeft(),
            unitWatermarkName, unitPartition);
        log.debug("Checking work unit: {}", wuSignature);

        // if a work unit exists in state store, manage its watermark independently
        Long unitCutoffTime = -1L;
        if (previousHighWatermarks.containsKey(wuSignature)) {
          unitCutoffTime = previousHighWatermarks.get(wuSignature)
              - MultistageProperties.MSTAGE_GRACE_PERIOD_DAYS.getMillis(sourceState)
              + MultistageProperties.MSTAGE_ABSTINENT_PERIOD_DAYS.getMillis(sourceState);
        }
        log.debug(String.format("previousHighWatermarks.get(wuSignature): %s, unitCutoffTime: %s",
            previousHighWatermarks.get(wuSignature), unitCutoffTime));

        // for a dated work unit partition, we only need to redo it when its previous
        // execution was not successful
        // for recent work unit partitions, we might need to re-extract based on
        // grace period logic, which is controlled by cut off time
        if (unitCutoffTime == -1L
            || dtPartition.getRight() >= Longs.max(unitCutoffTime, cutoffTime)) {
          // prune the date range only if it is not partitioned
          // note the nominal date range low boundary had been saved in signature
          ImmutablePair<Long, Long> dtPartitionModified = dtPartition;
          if (datetimePartitions.size() == 1 && dtPartition.left < cutoffTime) {
            dtPartitionModified = new ImmutablePair<>(cutoffTime, dtPartition.right);
          }
          log.debug("dtPartitionModified: {}", dtPartitionModified);

          log.info("Generating Work Unit: {}, watermark: {}", wuSignature, dtPartitionModified);
          WorkUnit workUnit = WorkUnit.create(extract,
              new WatermarkInterval(
                  new LongWatermark(dtPartitionModified.getLeft()),
                  new LongWatermark(dtPartitionModified.getRight())));

          // save work unit signature for identification
          // because each dataset URN key will have a state file on Hadoop, it cannot contain path separator
          workUnit.setProp(MultistageProperties.MSTAGE_WATERMARK_GROUPS.toString(),
              watermarkGroups.toString());
          workUnit.setProp(MultistageProperties.DATASET_URN_KEY.toString(), EndecoUtils.getHadoopFsEncoded(wuSignature));

          // save the lower number of datetime watermark partition and the unit watermark partition
          workUnit.setProp(datetimeWatermarkName, dtPartition.getLeft());
          workUnit.setProp(unitWatermarkName, unitPartition);

          workUnit.setProp(MultistageProperties.MSTAGE_ACTIVATION_PROPERTY.toString(), unitPartition);
          workUnit.setProp(MultistageProperties.MSTAGE_WORKUNIT_STARTTIME_KEY.toString(),
              DateTime.now().getMillis()
                  + workUnits.size() * MultistageProperties.MSTAGE_WORK_UNIT_PACING_SECONDS.getMillis(sourceState));

          if (!MultistageProperties.MSTAGE_OUTPUT_SCHEMA.validateNonblank(sourceState)
            && this.jobKeys.hasOutputSchema()) {
            // populate the output schema read from URN reader to sub tasks
            // so that the URN reader will not be called again
            log.info("Populating output schema to work units:");
            log.info("Output schema: {}", this.jobKeys.getOutputSchema()
                .getSchema().get(KEY_WORD_ITEMS).getAsJsonArray().toString());
            workUnit.setProp(MultistageProperties.MSTAGE_OUTPUT_SCHEMA.getConfig(),
                this.jobKeys.getOutputSchema()
                    .getSchema().get(KEY_WORD_ITEMS).getAsJsonArray().toString());
          }
          workUnits.add(workUnit);
        }
      }
    }
    return workUnits;
  }

  /**
   * breaks a date time range to smaller partitions per WORK_UNIT_PARTITION property setting
   * if too many partitions created, truncate to the maximum partitions allowed
   *  @param datetimeRange a range of date time values
   * @return a list of data time ranges in milliseconds
   */
  private List<ImmutablePair<Long, Long>> getDatetimePartitions(ImmutablePair<DateTime, DateTime> datetimeRange) {
    List<ImmutablePair<Long, Long>> partitions = Lists.newArrayList();
    if (jobKeys.getWorkUnitPartitionType() != null) {
      partitions = jobKeys.getWorkUnitPartitionType().getRanges(datetimeRange,
          MultistageProperties.MSTAGE_WORK_UNIT_PARTIAL_PARTITION.getValidNonblankWithDefault(sourceState));
    } else {
      partitions.add(new ImmutablePair<>(datetimeRange.getLeft().getMillis(), datetimeRange.getRight().getMillis()));
    }
    // Safety check if too many partitions created
    if (partitions.size() > MAX_DATETIME_PARTITION) {
      // Preserve the last N partitions
      partitions = partitions.subList(partitions.size() - MAX_DATETIME_PARTITION, partitions.size());
      log.warn("Too many partitions, created {}, only processing the last {}", partitions.size(), MAX_DATETIME_PARTITION);
    }
    return partitions;
  }

  /**
   * Read preceding job output, and return as a JsonArray.
   *
   * The location of preceding job output and fields of selection are
   * configured in parameter multistagesource.secondary.input, which should
   * have a path element and fields element. The path element shall contain
   * a list of input paths, and the fields element contains columns to be
   * returned.
   *
   * Assume this cannot be a Json primitive, and return null if so.
   *
   * @return a set of JsonArrays of data read from locations specified in SECONDARY_INPUT
   *         property organized by category, in a Map<String, JsonArray> structure
   */
  private Map<String, JsonArray> readContext(State state) {
    return new HdfsReader(state, jobKeys.getSecondaryInputs()).readAll();
  }

  /**
   * Get all previous highest high watermarks, by dataset URN. If a dataset URN
   * had multiple work units, the highest high watermark is retrieved for that
   * dataset URN.
   *
   * @return the previous highest high watermarks by dataset URN
   */
  private Map<String, Long> getPreviousHighWatermarks() {
    Map<String, Long> watermarks = new HashMap<>();
    Map<String, Iterable<WorkUnitState>> wuStates = sourceState.getPreviousWorkUnitStatesByDatasetUrns();
    for (String k: wuStates.keySet()) {
      Long highWatermark = Collections.max(Lists.newArrayList(wuStates.get(k).iterator()).stream()
          .map(s -> s.getActualHighWatermark(LongWatermark.class).getValue())
          .collect(Collectors.toList()));

      // Unit watermarks might contain encoded file separator,
      // in such case, we will decode the watermark name so that it can be compared with
      // work unit signatures
      log.debug("Dataset Signature: {}, High Watermark: {}", EndecoUtils.getHadoopFsDecoded(k), highWatermark);
      watermarks.put(EndecoUtils.getHadoopFsDecoded(k), highWatermark);
    }
    return ImmutableMap.copyOf(watermarks);
  }

  Extract createExtractObject(final boolean isFull) {
    Extract extract = createExtract(
        Extract.TableType.valueOf(MultistageProperties.EXTRACT_TABLE_TYPE_KEY.getValidNonblankWithDefault(sourceState)),
        MultistageProperties.EXTRACT_NAMESPACE_NAME_KEY.getProp(sourceState),
        MultistageProperties.EXTRACT_TABLE_NAME_KEY.getProp(sourceState));
    extract.setProp(ConfigurationKeys.EXTRACT_IS_FULL_KEY, isFull);
    return extract;
  }

  private String getWorkUnitSignature(
      String datetimeWatermarkName,
      Long datetimePartition,
      String unitWatermarkName,
      String unitPartition) {
    List<String> list = Lists.newArrayList(datetimeWatermarkName + PROPERTY_SEPARATOR + datetimePartition,
        unitWatermarkName + PROPERTY_SEPARATOR  + unitPartition);
    return list.toString();
  }

  /**
   * Output schema can come from 2 sources
   *
   * 1. actual schema defined in ms.output.schema parameter
   * 2. a URN or source defined in ms.source.schema.urn
   *
   * @param state the Gobblin configurations
   * @return the output schema
   */
  public JsonSchema getOutputSchema(State state, String urn) {
    JsonArray schema = new JsonArray();
    if (!jobKeys.hasOutputSchema() && StringUtils.isNotBlank(urn)) {
      schema = readOutputSchemaFromUrn(schemaReaderFactory, urn, state);
      return new JsonSchema().addMember(KEY_WORD_ITEMS,
          JsonUtils.deepCopy(schema).getAsJsonArray());
    }
    return jobKeys.getOutputSchema();
  }

  /**
   * Call the reader factory and read schema of the URN
   * @param factory the reader factory
   * @param urn the dataset URN
   * @param state gobblin configuration
   * @return schema in a JsonArray
   */
  @VisibleForTesting
  JsonArray readOutputSchemaFromUrn(SchemaReaderFactory factory, String urn, State state) {
    try {
      JsonArray fromReader = factory.read(state, urn).getAsJsonArray();
      // remove derived fields
      Set<String> derived = this.jobKeys.getDerivedFields().keySet();
      if (derived.isEmpty()) {
        return fromReader;
      }

      JsonArray output = new JsonArray();
      for (JsonElement column: fromReader) {
        if (!derived.contains(column.getAsJsonObject().get(KEY_WORD_COLUMN_NAME).getAsString())) {
          output.add(column);
        }
      }
      return output;
    } catch (Exception e) {
      log.error("Error reading schema based on urn: {}", urn);
      log.error("This is usually caused by incorrect URN or connection issues to the schema source");
      log.error("If the URN is unintended, please remove the configuration {}",
          MultistageProperties.MSTAGE_SOURCE_SCHEMA_URN.getConfig());
      throw new RuntimeException(e);
    }
  }

  /**
   * Creating a schema reader, default reads from TMS
   * @param state Gobblin configuration
   * @return the reader factory
   */
  @VisibleForTesting
  SchemaReaderFactory createSchemaReaderFactory(State state) {
    try {
      Class<?> factoryClass = Class.forName(
          MultistageProperties.MSTAGE_SOURCE_SCHEMA_READER_FACTORY.getValidNonblankWithDefault(state));
      this.schemaReaderFactory = (SchemaReaderFactory) factoryClass.newInstance();
      return this.schemaReaderFactory;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * retrieve the authentication data from secondary input
   * TODO there is a slight inefficiency here
   * @param retries number of retries remaining
   * @return the authentication JsonObject
   */
  @SneakyThrows
  protected JsonObject readSecondaryAuthentication(State state, final long retries) {
    Map<String, JsonArray> secondaryInputs = readSecondaryInputs(state, retries);
    if (secondaryInputs.containsKey(KEY_WORD_ACTIVATION)
        && secondaryInputs.get(KEY_WORD_AUTHENTICATION).isJsonArray()
        && secondaryInputs.get(KEY_WORD_AUTHENTICATION).getAsJsonArray().size() > 0) {
      return secondaryInputs.get(KEY_WORD_AUTHENTICATION).get(0).getAsJsonObject();
    }
    return new JsonObject();
  }

  /**
   * This updates the activation properties of the work unit if a new authentication token
   * become available
   * @param wu the work unit configuration
   * @param authentication the authentication token from, usually, the secondary input
   * @return the updated work unit configuration
   */
  protected String getUpdatedWorkUnitActivation(WorkUnit wu, JsonObject authentication) {
    log.debug("Activation property (origin): {}", wu.getProp(MultistageProperties.MSTAGE_ACTIVATION_PROPERTY.toString(), ""));
    if (!wu.getProp(MultistageProperties.MSTAGE_ACTIVATION_PROPERTY.toString(), StringUtils.EMPTY).isEmpty()) {
      JsonObject existing = GSON.fromJson(wu.getProp(MultistageProperties.MSTAGE_ACTIVATION_PROPERTY.toString()), JsonObject.class);
      for (Map.Entry<String, JsonElement> entry: authentication.entrySet()) {
        existing.remove(entry.getKey());
        existing.addProperty(entry.getKey(), entry.getValue().getAsString());
      }
      log.debug("Activation property (modified): {}", existing.toString());
      return existing.toString();
    }
    log.debug("Activation property (new): {}", authentication.toString());
    return authentication.toString();
  }

  /**
   * Check if a full extract is needed
   * @param state source state
   * @param previousHighWatermarks existing high watermarks
   * @return true if all conditions met for a full extract, otherwise false
   */
  private boolean checkFullExtractState(final State state, final Map<String, Long> previousHighWatermarks) {
    if (MultistageProperties.EXTRACT_TABLE_TYPE_KEY.getValidNonblankWithDefault(state).toString()
        .equalsIgnoreCase(KEY_WORD_SNAPSHOT_ONLY)) {
      return true;
    }

    if (MultistageProperties.MSTAGE_ENABLE_DYNAMIC_FULL_LOAD.getValidNonblankWithDefault(state)) {
      if (previousHighWatermarks.isEmpty()) {
        return true;
      }
    }

    return state.getPropAsBoolean(ConfigurationKeys.EXTRACT_IS_FULL_KEY, false);
  }
}

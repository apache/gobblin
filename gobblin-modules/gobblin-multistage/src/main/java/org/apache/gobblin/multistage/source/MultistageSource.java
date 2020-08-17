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
import com.google.gson.reflect.TypeToken;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.multistage.configuration.MultistageProperties;
import org.apache.gobblin.multistage.extractor.MultistageExtractor;
import org.apache.gobblin.multistage.keys.SourceKeys;
import org.apache.gobblin.multistage.util.DateTimeUtils;
import org.apache.gobblin.multistage.util.HdfsReader;
import org.apache.gobblin.multistage.util.JsonParameter;
import org.apache.gobblin.multistage.util.JsonSchema;
import org.apache.gobblin.multistage.util.JsonUtils;
import org.apache.gobblin.multistage.util.ParameterTypes;
import org.apache.gobblin.multistage.util.VariableUtils;
import org.apache.gobblin.multistage.util.WatermarkDefinition;
import org.apache.gobblin.multistage.util.WorkUnitPartitionTypes;
import org.apache.gobblin.multistage.util.WorkUnitStatus;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.WatermarkInterval;
import org.apache.gobblin.source.extractor.extract.AbstractSource;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;

/**
 * This is the base Source class of multi-stage connectors.
 *
 * Multi-stage connectors can pass intermediate data between jobs or stages within
 * a job.
 *
 * Multi-stage source isolate the connector logic and function as broker to
 * data sources.
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
  final private static String KEY_WORD_CATEGORY = "category";
  final private static String KEY_WORD_RETRY = "retry";
  final private static String KEY_WORD_RETRY_DELAY_IN_SEC = "delayInSec";
  final private static String KEY_WORD_RETRY_COUNT = "retryCount";
  final private static String KEY_WORD_SNAPSHOT_ONLY = "SNAPSHOT_ONLY";
  final private static int RETRY_DELAY_IN_SEC_DEFAULT = 300;
  final private static int RETRY_COUNT_DEFAULT = 3;
  // Avoid too many partition created from misconfiguration, Months * Days * Hours
  final private static int MAX_DATETIME_PARTITION = 3 * 30 * 24;

  @Getter(AccessLevel.PUBLIC)
  protected SourceState sourceState = null;
  protected ConcurrentMap<MultistageExtractor, WorkUnitStatus> memberExtractorStatus = new ConcurrentHashMap<>();
  protected ConcurrentMap<MultistageExtractor, JsonObject> memberParameters = new ConcurrentHashMap<>();
  @Getter(AccessLevel.PUBLIC)
  @Setter(AccessLevel.PUBLIC)
  SourceKeys sourceKeys = new SourceKeys();

  public WorkUnitStatus getFirst(MultistageExtractor extractor) {
    logUsage(extractor.getState());
    memberParameters.put(extractor, getInitialWorkUnitParameters(extractor));
    memberExtractorStatus.put(extractor, extractor.getWorkUnitStatus().toBuilder().build());
    return memberExtractorStatus.get(extractor);
  }

  public WorkUnitStatus getNext(MultistageExtractor extractor) {
    try {
      Thread.sleep(sourceKeys.getCallInterval());
    } catch (Exception e) {
      log.warn(e.getMessage());
    }
    log.info("Starting a new request to the source, work unit = {}", extractor.getExtractorKeys().getSignature());
    if (memberParameters.containsKey(extractor)) {
      log.debug("Prior parameters: {}", memberParameters.get(extractor).toString());
    }
    if (memberExtractorStatus.containsKey(extractor)) {
      log.debug("Prior work unit status: {}", memberExtractorStatus.get(extractor).toString());
    }
    memberParameters.put(extractor, getCurrentWorkUnitParameters(extractor));
    memberExtractorStatus.put(extractor, extractor.getWorkUnitStatus().toBuilder().build());
    return memberExtractorStatus.get(extractor);
  }

  public void closeStream(MultistageExtractor extractor) {
    log.info("Closing InputStream for {}", extractor.getExtractorKeys().getSignature());
  }

  protected void initialize(State state) {
    getPaginationFields(state);
    getPaginationInitialValues(state);
    sourceKeys.setSessionKeyField(MultistageProperties.MSTAGE_SESSION_KEY_FIELD.getValidNonblankWithDefault(state));
    sourceKeys.setTotalCountField(MultistageProperties.MSTAGE_TOTAL_COUNT_FIELD.getValidNonblankWithDefault(state));
    sourceKeys.setSourceParameters(MultistageProperties.MSTAGE_PARAMETERS.getValidNonblankWithDefault(state));
    sourceKeys.setDefaultFieldTypes(getDefaultFieldTypes(state));
    sourceKeys.setOutputSchema(parseOutputSchema(state));
    sourceKeys.setDerivedFields(getDerivedFields(state));
    sourceKeys.setEncryptionField(MultistageProperties.MSTAGE_ENCRYPTION_FIELDS.getValidNonblankWithDefault(state));
    sourceKeys.setDataField(MultistageProperties.MSTAGE_DATA_FIELD.getValidNonblankWithDefault(state));
    sourceKeys.setCallInterval(MultistageProperties.MSTAGE_CALL_INTERVAL.getProp(state));
    sourceKeys.setSessionTimeout(MultistageProperties.MSTAGE_WAIT_TIMEOUT_SECONDS.getMillis(state));
    sourceKeys.setEnableCleansing(MultistageProperties.MSTAGE_ENABLE_CLEANSING.getValidNonblankWithDefault(state));
    sourceKeys.setIsPartialPartition(MultistageProperties.MSTAGE_WORK_UNIT_PARTIAL_PARTITION.getValidNonblankWithDefault(state));
    sourceKeys.setWorkUnitPartitionType(getPartitionType(state));
    sourceKeys.setWatermarkDefinition(MultistageProperties.MSTAGE_WATERMARK.getValidNonblankWithDefault(state));
    Map<String, Long> retry = parseSecondaryInputRetry(
        MultistageProperties.MSTAGE_SECONDARY_INPUT.getValidNonblankWithDefault(state));
    sourceKeys.setRetryDelayInSec(retry.get(KEY_WORD_RETRY_DELAY_IN_SEC));
    sourceKeys.setRetryCount(retry.get(KEY_WORD_RETRY_COUNT));
    sourceKeys.setSecondaryInputs(MultistageProperties.MSTAGE_SECONDARY_INPUT.getValidNonblankWithDefault(state));
  }

  /**
   *  This method populates the retry parameters (delayInSec, retryCount) via the secondary input.
   *   These values are used to retry connection whenever the "authentication" type category is defined and the token hasn't
   *   been populated yet. If un-defined, they will retain the default values as specified by RETRY_DEFAULT_DELAY and
   *   RETRY_DEFAULT_COUNT.
   *
   *   For e.g.
   *   ms.secondary.input : "[{"path": "/util/avro_retry", "fields": ["uuid"],
   *   "category": "authentication", "retry": {"delayInSec" : "1", "retryCount" : "2"}}]"
   * @param jsonArray the raw secondary input
   * @return the retry delay and count in a map structure
   */
  private Map<String, Long> parseSecondaryInputRetry(JsonArray jsonArray) {
    long retryDelay = RETRY_DELAY_IN_SEC_DEFAULT;
    long retryCount = RETRY_COUNT_DEFAULT;
    Map<String, Long> retry = new HashMap<>();
    for (JsonElement field: jsonArray) {
      JsonObject retryFields = new JsonObject();
      retryFields = (JsonObject) field.getAsJsonObject().get(KEY_WORD_RETRY);
      if (retryFields != null && !retryFields.isJsonNull()) {
        retryDelay = retryFields.has(KEY_WORD_RETRY_DELAY_IN_SEC)
            ? retryFields.get(KEY_WORD_RETRY_DELAY_IN_SEC).getAsLong() : retryDelay;
        retryCount = retryFields.has(KEY_WORD_RETRY_COUNT)
            ? retryFields.get(KEY_WORD_RETRY_COUNT).getAsLong() : retryCount;
      }
    }
    retry.put(KEY_WORD_RETRY_DELAY_IN_SEC, retryDelay);
    retry.put(KEY_WORD_RETRY_COUNT, retryCount);
    return retry;
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
    sourceKeys.logDebugAll();

    if (!sourceKeys.validate(state)) {
      return new ArrayList<>();
    }

    // Parse watermark settings if defined
    List<WatermarkDefinition> definedWatermarks = Lists.newArrayList();
    for (JsonElement definitionJson : sourceKeys.getWatermarkDefinition()) {
      definedWatermarks.add(new WatermarkDefinition(
          definitionJson.getAsJsonObject(), sourceKeys.getIsPartialPartition(),
          sourceKeys.getWorkUnitPartitionType()));
    }

    Map<String, JsonArray> secondaryInputs = readSecondaryInputs(sourceState, sourceKeys.getRetryCount());
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
        && isSecondaryAuthenticationEnabled() && retries > 0) {
      log.info("Authentication tokens are expected from secondary input, but not ready");
      log.info("Will wait for {} seconds and then retry reading the secondary input", sourceKeys.getRetryDelayInSec());
      TimeUnit.SECONDS.sleep(sourceKeys.getRetryDelayInSec());
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
  public Extractor getExtractor(WorkUnitState state) {
    try {
      ClassLoader loader = this.getClass().getClassLoader();
      Class extractorClass = loader.loadClass(MultistageProperties.MSTAGE_EXTRACTOR_CLASS.getValidNonblankWithDefault(state));
      Constructor<MultistageExtractor>
          constructor = extractorClass.getConstructor(WorkUnitState.class, MultistageSource.class);
      return constructor.newInstance(state, this);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void shutdown(SourceState state) {

  }

  List<WorkUnit> generateWorkUnits(List<WatermarkDefinition> definitions, Map<String, Long> previousHighWatermarks) {
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

          log.info("Generating Work Unit: {}, watermark: {}", wuSignature, dtPartitionModified);
          WorkUnit workUnit = WorkUnit.create(extract,
              new WatermarkInterval(
                  new LongWatermark(dtPartitionModified.getLeft()),
                  new LongWatermark(dtPartitionModified.getRight())));

          // save work unit signature for identification
          // because each dataset URN key will have a state file on Hadoop, it cannot contain path separator
          workUnit.setProp(MultistageProperties.MSTAGE_WATERMARK_GROUPS.toString(),
              watermarkGroups.toString());
          workUnit.setProp(ConfigurationKeys.DATASET_URN_KEY, getHadoopFsEncoded(wuSignature));

          // save the lower number of datetime watermark partition and the unit watermark partition
          workUnit.setProp(datetimeWatermarkName, dtPartition.getLeft());
          workUnit.setProp(unitWatermarkName, unitPartition);

          workUnit.setProp(MultistageProperties.MSTAGE_ACTIVATION_PROPERTY.toString(), unitPartition);
          workUnit.setProp(MultistageProperties.MSTAGE_WORKUNIT_STARTTIME_KEY.toString(),
              DateTime.now().getMillis()
                  + workUnits.size() * MultistageProperties.MSTAGE_WORK_UNIT_PACING_SECONDS.getMillis(sourceState));
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
    if (sourceKeys.getWorkUnitPartitionType() != null) {
      partitions = sourceKeys.getWorkUnitPartitionType().getRanges(datetimeRange,
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
    return new HdfsReader(state, sourceKeys.getSecondaryInputs()).readAll();
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
      log.debug("Dataset Signature: {}, High Watermark: {}", getHadoopFsDecoded(k), highWatermark);
      watermarks.put(getHadoopFsDecoded(k), highWatermark);
    }
    return ImmutableMap.copyOf(watermarks);
  }

  Extract createExtractObject(final boolean isFull) {
    Extract extract = createExtract(
        Extract.TableType.valueOf(sourceState.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY, "SNAPSHOT_ONLY")),
        sourceState.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY),
        sourceState.getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY));
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

  private void getPaginationFields(State state) {
    List<ParameterTypes> paramTypes = Lists.newArrayList(
        ParameterTypes.PAGESTART,
        ParameterTypes.PAGESIZE,
        ParameterTypes.PAGENO
    );
    if (MultistageProperties.MSTAGE_PAGINATION.validateNonblank(state)) {
      JsonObject p = MultistageProperties.MSTAGE_PAGINATION.getProp(state);
      if (p.has("fields")) {
        JsonArray fields = p.get("fields").getAsJsonArray();
        for (int i = 0; i < fields.size(); i++) {
          if (StringUtils.isNoneBlank(fields.get(i).getAsString())) {
            sourceKeys.getPaginationFields().put(paramTypes.get(i), fields.get(i).getAsString());
          }
        }
      }
    }
  }

  private void getPaginationInitialValues(State state) {
    List<ParameterTypes> paramTypes = Lists.newArrayList(
        ParameterTypes.PAGESTART,
        ParameterTypes.PAGESIZE,
        ParameterTypes.PAGENO
    );
    if (MultistageProperties.MSTAGE_PAGINATION.validateNonblank(state)) {
      JsonObject p = MultistageProperties.MSTAGE_PAGINATION.getProp(state);
      if (p.has("initialvalues")) {
        JsonArray values = p.get("initialvalues").getAsJsonArray();
        for (int i = 0; i < values.size(); i++) {
          sourceKeys.getPaginationInitValues().put(paramTypes.get(i), values.get(i).getAsLong());
        }
      }
    } else {
      sourceKeys.setPaginationInitValues(new HashMap<>());
    }
  }

  /**
   * Sample derived field configuration:
   * [{"name": "activityDate", "formula": {"type": "epoc", "source": "fromDateTime", "format": "yyyy-MM-dd'T'HH:mm:ss'Z'"}}]
   *
   * Currently, only "epoc" and "string" are supported as derived field type.
   * For epoc type:
   * - Data will be saved as milliseconds in long data type.
   * - And the source data is supposed to be a date formatted as a string.
   *
   * TODO: support more types.
   *
   * @return derived fields and their definitions
   */
  private Map<String, Map<String, String>> getDerivedFields(State state) {
    if (!MultistageProperties.MSTAGE_DERIVED_FIELDS.validateNonblank(state)) {
      return new HashMap<>();
    }

    Map<String, Map<String, String>> derivedFields = new HashMap<>();
    JsonArray jsonArray = MultistageProperties.MSTAGE_DERIVED_FIELDS.getProp(state);
    for (JsonElement field: jsonArray) {

      // change the formula part, which is JsonObject, into map
      derivedFields.put(field.getAsJsonObject().get("name").getAsString(),
          GSON.fromJson(
              field.getAsJsonObject().get("formula").getAsJsonObject().toString(),
              new TypeToken<HashMap<String, String>>() { }.getType()));
    }

    return derivedFields;
  }

  /**
   * Default field types can be used in schema inferrence, this method
   * collect default field types if they are  specified in configuration.
   *
   * @return A map of fields and their default types
   */
  private Map<String, String> getDefaultFieldTypes(State state) {
    if (MultistageProperties.MSTAGE_DATA_DEFAULT_TYPE.validateNonblank(state)) {
      return GSON.fromJson(MultistageProperties.MSTAGE_DATA_DEFAULT_TYPE.getProp(state).toString(),
          new TypeToken<HashMap<String, String>>() {
          }.getType());
    }
    return new HashMap<>();
  }

  public JsonSchema parseOutputSchema(State state) {
    JsonSchema outputSchema = new JsonSchema();
    if (MultistageProperties.MSTAGE_OUTPUT_SCHEMA.validateNonblank(state)) {
      JsonArray schemaArray = MultistageProperties.MSTAGE_OUTPUT_SCHEMA.getProp(state);
      outputSchema.addMember("items", JsonUtils.deepCopy(schemaArray).getAsJsonArray());
    }
    return outputSchema;
  }

  /**
   * Initial variable values are not specific to protocols, moving this method here
   * so that it can be shared among protocols.
   *
   * Initial work unit variable values include
   * - watermarks defined for each work unit
   * - initial pagination defined at the source level
   *
   * @param extractor the extractor object
   * @return work unit specific initial parameters for the first request to source
   */
  private JsonObject getInitialWorkUnitVariableValues(MultistageExtractor extractor) {
    JsonObject variableValues = new JsonObject();

    variableValues.add(ParameterTypes.WATERMARK.toString(), extractor.getWorkUnitWaterMarks());
    for (Map.Entry<ParameterTypes, Long> entry: sourceKeys.getPaginationInitValues().entrySet()) {
      variableValues.addProperty(entry.getKey().toString(), entry.getValue());
    }
    return variableValues;
  }

  /**
   * Update variable values based on work unit status
   *
   * Following variable values are updated:
   * 1. session key value if the work unit status has session key
   * 2. page start value if page start control is used
   * 3. page size value if page size control is used
   * 4. page number value if page number control is used
   *
   * Typically use case can use any of following pagination methods, some may use multiple:
   * 1. use page start (offset) and page size to control pagination
   * 2. use page number and page size to control pagination
   * 3. use page number to control pagination, while page size can be fixed
   * 4. use session key to control pagination, and the session key decides what to fetch next
   * 5. not use any variables, the session just keep going until following conditions are met:
   *    a. return an empty page
   *    b. return a specific status, such as "complete", in response
   *
   * @param extractor the work unit
   * @param initialVariableValues initial variable values
   * @return the updated variable values
   */
  private JsonObject getUpdatedWorkUnitVariableValues(
      MultistageExtractor extractor,
      JsonObject initialVariableValues) {
    JsonObject updatedVariableValues = JsonUtils.deepCopy(initialVariableValues).getAsJsonObject();
    WorkUnitStatus wuStatus = extractor.getWorkUnitStatus();

    // if session key is used, the extractor has to provide it int its work unit status
    // in order for this to work
    if (updatedVariableValues.has(ParameterTypes.SESSION.toString())) {
      updatedVariableValues.remove(ParameterTypes.SESSION.toString());
    }
    updatedVariableValues.addProperty(ParameterTypes.SESSION.toString(),
        wuStatus.getSessionKey());

    // if page start is used, the extractor has to provide it int its work unit status
    // in order for this to work
    if (updatedVariableValues.has(ParameterTypes.PAGESTART.toString())) {
      updatedVariableValues.remove(ParameterTypes.PAGESTART.toString());
    }
    updatedVariableValues.addProperty(ParameterTypes.PAGESTART.toString(),
        wuStatus.getPageStart());

    // page size doesn't change much often, if extractor doesn't provide
    // a page size, then assume it is the same as initial value
    if (updatedVariableValues.has(ParameterTypes.PAGESIZE.toString())
        && wuStatus.getPageSize() > 0) {
      updatedVariableValues.remove(ParameterTypes.PAGESIZE.toString());
    }
    if (wuStatus.getPageSize() > 0) {
      updatedVariableValues.addProperty(ParameterTypes.PAGESIZE.toString(),
          wuStatus.getPageSize());
    }

    // if page number is used, the extractor has to provide it in its work unit status
    // in order for this to work
    if (updatedVariableValues.has(ParameterTypes.PAGENO.toString())) {
      updatedVariableValues.remove(ParameterTypes.PAGENO.toString());
    }
    updatedVariableValues.addProperty(ParameterTypes.PAGENO.toString(),
        wuStatus.getPageNumber());

    return updatedVariableValues;
  }

  /**
   * ms.parameters have variables. For the initial execution of each work unit, we substitute those
   * variables with initial work unit variable values.
   *
   * @param extractor the extractor object representing a work unit
   * @return the substituted parameters
   */
  protected JsonObject getInitialWorkUnitParameters(MultistageExtractor extractor) {
    JsonObject definedParameters = JsonParameter.getParametersAsJson(
        sourceKeys.getSourceParameters().toString(),
        getInitialWorkUnitVariableValues(extractor),
        extractor.getState());
    return replaceVariablesInParameters(
        appendActivationParameter(extractor, definedParameters));
  }

  protected JsonObject getCurrentWorkUnitParameters(MultistageExtractor extractor) {
    JsonObject definedParameters = JsonParameter.getParametersAsJson(
        sourceKeys.getSourceParameters().toString(),
        getUpdatedWorkUnitVariableValues(extractor, getInitialWorkUnitVariableValues(extractor)),
        extractor.getState());
    return replaceVariablesInParameters(
        appendActivationParameter(extractor, definedParameters));
  }

  /**
   * Add activation parameters to work unit parameters
   * @param extractor the extractor object representing a work unit
   * @param parameters the defined parameters
   * @return the set of parameters including activation parameters
   */
  private JsonObject appendActivationParameter(MultistageExtractor extractor, JsonObject parameters) {
    JsonObject activationParameters = extractor.getExtractorKeys().getActivationParameters();
    if (activationParameters.entrySet().size() > 0) {
      for (Map.Entry<String, JsonElement> entry: activationParameters.entrySet()) {
        String key = entry.getKey();
        parameters.add(key, activationParameters.get(key));
      }
    }
    return JsonUtils.deepCopy(parameters).getAsJsonObject();
  }

  /**
   * Decode an encoded URL string, complete or partial
   * @param encoded encoded URL string
   * @return decoded URL string
   */
  protected String decode(String encoded) {
    try {
      return URLDecoder.decode(encoded, StandardCharsets.UTF_8.toString());
    } catch (Exception e) {
      log.error("URL decoding error: " + e);
      return encoded;
    }
  }

  /**
   * Encode a URL string, complete or partial
   * @param plainUrl unencoded URL string
   * @return encoded URL string
   */
  @VisibleForTesting
  protected String getEncodedUtf8(String plainUrl) {
    try {
      return URLEncoder.encode(plainUrl, StandardCharsets.UTF_8.toString());
    } catch (Exception e) {
      log.error("URL encoding error: " + e);
      return plainUrl;
    }
  }

  /**
   * Encode a Hadoop file name to encode path separator so that the file name has no '/'
   * @param fileName unencoded file name string
   * @return encoded path string
   */
  @VisibleForTesting
  protected String getHadoopFsEncoded(String fileName) {
    try {
      String encodedSeparator = URLEncoder.encode(Path.SEPARATOR, StandardCharsets.UTF_8.toString());

      // we don't encode the whole string intentionally so that the state file name is more readable
      return fileName.replace(Path.SEPARATOR, encodedSeparator);
    } catch (Exception e) {
      log.error("Hadoop FS encoding error: " + e);
      return fileName;
    }
  }

  /**
   * Decode an encoded Hadoop file name to restore path separator
   * @param encodedFileName encoded file name string
   * @return encoded path string
   */
  @VisibleForTesting
  protected String getHadoopFsDecoded(String encodedFileName) {
    try {
      String encodedSeparator = URLEncoder.encode(Path.SEPARATOR, StandardCharsets.UTF_8.toString());
      return encodedFileName.replace(encodedSeparator, Path.SEPARATOR);
    } catch (Exception e) {
      log.error("Hadoop FS decoding error: " + e);
      return encodedFileName;
    }
  }

  /**
   * This method applies the work unit parameters to string template, and
   * then return a work unit specific string
   *
   * @param template the template string
   * @param parameters the parameters with all variables substituted
   * @return work unit specific string
   */
  protected String getWorkUnitSpecificString(String template, JsonObject parameters) {
    String finalString = template;
    try {
      // substitute with parameters defined in ms.parameters and activation parameters
      finalString = VariableUtils.replaceWithTracking(
          finalString,
          parameters,
          false).getKey();
    } catch (Exception e) {
      log.error("Error getting work unit specific string " + e);
    }
    log.info("Final work unit specific string: {}", finalString);
    return finalString;
  }

  /**
   * Replace variables in the parameters itself, so that ms.parameters can accept variables.
   * @param parameters the JsonObject with parameters
   * @return the replaced parameter object
   */
  JsonObject replaceVariablesInParameters(final JsonObject parameters) {
    JsonObject parametersCopy = JsonUtils.deepCopy(parameters).getAsJsonObject();
    JsonObject finalParameter = JsonUtils.deepCopy(parameters).getAsJsonObject();;
    try {
      Pair<String, JsonObject> replaced = VariableUtils.replaceWithTracking(
          parameters.toString(), parametersCopy, false);
      finalParameter = GSON.fromJson(replaced.getKey(), JsonObject.class);

      // for each parameter in the original parameter list, if the name of the parameter
      // name starts with "tmp" and the parameter was used once in this substitution operation,
      // then it shall be removed from the final list
      for (Map.Entry<String, JsonElement> entry: parameters.entrySet()) {
        if (entry.getKey().matches("tmp.*")
            && !replaced.getRight().has(entry.getKey())) {
          finalParameter.remove(entry.getKey());
        }
      }
    } catch (Exception e) {
      log.error("Encoding error is not expected, but : {}", e.getMessage());
    }
    log.debug("Final parameters: {}", finalParameter.toString());
    return finalParameter;
  }

  protected void logUsage(State state) {
    log.info("Checking essential (not always mandatory) parameters...");
    log.info("Values can be default values for the specific type if the property is not configured");
    for (MultistageProperties p: SourceKeys.ESSENTIAL_PARAMETERS) {
      log.info("Property {} ({}) has value {} ", p.toString(), p.getClassName(), p.getValidNonblankWithDefault(state));
    }
  }

  /**
   * check if authentication is configured in secondary input
   * @return true if secondary input contains an authentication definition
   */
  protected boolean isSecondaryAuthenticationEnabled() {
    for (JsonElement entry: sourceKeys.getSecondaryInputs()) {
      if (entry.isJsonObject()
          && entry.getAsJsonObject().has(KEY_WORD_CATEGORY)
          && entry.getAsJsonObject().get(KEY_WORD_CATEGORY).getAsString()
          .equalsIgnoreCase(KEY_WORD_AUTHENTICATION)) {
        return true;
      }
    }
    return false;
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
  boolean checkFullExtractState(final State state, final Map<String, Long> previousHighWatermarks) {
    String extractTableType = state.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY, KEY_WORD_SNAPSHOT_ONLY);
    if (extractTableType == null || extractTableType.equalsIgnoreCase(KEY_WORD_SNAPSHOT_ONLY)) {
      return true;
    }

    if (MultistageProperties.MSTAGE_ENABLE_DYNAMIC_FULL_LOAD.getValidNonblankWithDefault(state)) {
      if (previousHighWatermarks.isEmpty()) {
        return true;
      }
    }

    return state.getPropAsBoolean(ConfigurationKeys.EXTRACT_IS_FULL_KEY, false);
  }

  /**
   * Convert a list of strings to an InputStream
   * @param stringList a list of strings
   * @return an InputStream made of the list
   */
  protected InputStream convertListToInputStream(List<String> stringList) {
    if (stringList == null || stringList.size() == 0) {
      return null;
    }
    return new ByteArrayInputStream(String.join("\n", stringList).getBytes());
  }

  /**
   * This helper function parse out the WorkUnitPartitionTypes from ms.work.unit.partition property
   * @param state the State with all configurations
   * @return the WorkUnitPartitionTypes
   */
  WorkUnitPartitionTypes getPartitionType(State state) {
    WorkUnitPartitionTypes partitionType = WorkUnitPartitionTypes.fromString(
        MultistageProperties.MSTAGE_WORK_UNIT_PARTITION.getValidNonblankWithDefault(state));

    if (partitionType != WorkUnitPartitionTypes.COMPOSITE) {
      return partitionType;
    }

    // add sub ranges for composite partition type
    WorkUnitPartitionTypes.COMPOSITE.resetSubRange();
    try {
      JsonObject jsonObject = GSON.fromJson(
          MultistageProperties.MSTAGE_WORK_UNIT_PARTITION.getValidNonblankWithDefault(state).toString(),
          JsonObject.class);

      for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
        String partitionTypeString = entry.getKey();
        DateTime start = DateTimeUtils.parse(jsonObject.get(entry.getKey()).getAsJsonArray().get(0).getAsString());
        String endDateTimeString = jsonObject.get(entry.getKey()).getAsJsonArray().get(1).getAsString();
        DateTime end;
        if (endDateTimeString.matches("-")) {
          end = DateTime.now();
        } else {
          end = DateTimeUtils.parse(endDateTimeString);
        }
        partitionType.addSubRange(start, end, WorkUnitPartitionTypes.fromString(partitionTypeString));
      }
    } catch (Exception e) {
      log.error("Error parsing composite partition string: "
          + MultistageProperties.MSTAGE_WORK_UNIT_PARTITION.getValidNonblankWithDefault(state).toString()
          + "\n partitions may not be generated properly.",
          e);
    }
    return partitionType;
  }
}

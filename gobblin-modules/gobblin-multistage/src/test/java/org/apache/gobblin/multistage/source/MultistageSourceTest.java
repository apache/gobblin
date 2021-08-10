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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.multistage.configuration.MultistageProperties;
import org.apache.gobblin.multistage.extractor.MultistageExtractor;
import org.apache.gobblin.multistage.keys.ExtractorKeys;
import org.apache.gobblin.multistage.keys.SourceKeys;
import org.apache.gobblin.multistage.util.ParameterTypes;
import org.apache.gobblin.multistage.util.WatermarkDefinition;
import org.apache.gobblin.multistage.util.WorkUnitPartitionTypes;
import org.apache.gobblin.multistage.util.WorkUnitStatus;
import org.apache.gobblin.source.extractor.WatermarkInterval;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class MultistageSourceTest {
  private final static DateTimeFormatter JODA_DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ");
  private Gson gson;
  private MultistageSource source;

  @BeforeMethod
  public void setUp() {
    gson = new Gson();
    source = new MultistageSource();
  }

  @Test
  public void testWorkUnitPartitionDef(){
    SourceState state = mock(SourceState.class);
    when(state.getProp("ms.work.unit.partition", "")).thenReturn("daily");
    when(state.getProp("ms.pagination", new JsonObject().toString())).thenReturn("{}");
    when(state.getProp(MultistageProperties.MSTAGE_OUTPUT_SCHEMA.getConfig(), "")).thenReturn("");
    when(state.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY, "SNAPSHOT_ONLY")).thenReturn("SNAPSHOT_ONLY");

    MultistageSource source = new MultistageSource();
    source.getWorkunits(state);

    String expected = "daily";
    Assert.assertEquals(expected, MultistageProperties.MSTAGE_WORK_UNIT_PARTITION.getProp(state));
  }

  @Test
  public void testWorkUnitPacingDef(){
    SourceState state = mock(SourceState.class);
    when(state.getPropAsInt("ms.work.unit.pacing.seconds", 0)).thenReturn(10);
    when(state.getProp("ms.pagination", new JsonObject().toString())).thenReturn("{}");
    when(state.getProp(MultistageProperties.MSTAGE_OUTPUT_SCHEMA.getConfig(), "")).thenReturn("");
    when(state.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY, "SNAPSHOT_ONLY")).thenReturn("SNAPSHOT_ONLY");
    MultistageSource source = new MultistageSource();
    source.getWorkunits(state);
    Assert.assertEquals(((Integer) MultistageProperties.MSTAGE_WORK_UNIT_PACING_SECONDS.getProp(state)).intValue(), 10);
  }

  @Test
  public void testWorkUnitPacingConversion(){
    SourceState state = mock(SourceState.class);
    when(state.getPropAsInt("ms.work.unit.pacing.seconds", 0)).thenReturn(10);
    when(state.getProp("ms.pagination", new JsonObject().toString())).thenReturn("{\"fields\": [\"start\"]}");
    when(state.getProp(MultistageProperties.MSTAGE_OUTPUT_SCHEMA.getConfig(), "")).thenReturn("");
    when(state.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY, "SNAPSHOT_ONLY")).thenReturn("SNAPSHOT_ONLY");
    MultistageSource source = new MultistageSource();
    source.getWorkunits(state);
    Assert.assertEquals(MultistageProperties.MSTAGE_WORK_UNIT_PACING_SECONDS.getMillis(state).longValue(), 10000L);
  }

  @Test
  public void testGetWorkUnitsTooManyPartitions() {
    SourceState state = new SourceState();
    state.setProp("ms.watermark",
        "[{\"name\": \"system\",\"type\": \"datetime\", \"range\": {\"from\": \"2000-01-01\", \"to\": \"-\"}}]");
    state.setProp("extract.table.type", "SNAPSHOT_ONLY");
    state.setProp("extract.namespace", "test");
    state.setProp("extract.table.name", "table1");
    state.setProp("ms.work.unit.partition", "hourly");
    state.setProp("ms.pagination", "{}");
    MultistageSource source = new MultistageSource();
    List<WorkUnit> wuList = source.getWorkunits(state);
    // Expected max partition allowed maps to MultistageSource.MAX_DATETIME_PARTITION
    Assert.assertEquals(wuList.size(), 3 * 30 * 24);
  }

  @Test
  public void testGetWorkUnitsDefault(){
    SourceState state = new SourceState();
    state.setProp("ms.watermark", "[{\"name\": \"system\",\"type\": \"datetime\", \"range\": {\"from\": \"2017-01-01\", \"to\": \"-\"}}]");
    state.setProp("extract.table.type", "SNAPSHOT_ONLY");
    state.setProp("extract.namespace", "test");
    state.setProp("extract.table.name", "table1");
    state.setProp("ms.work.unit.partition", "");
    state.setProp("ms.pagination", "{}");
    MultistageSource source = new MultistageSource();
    source.getWorkunits(state);

    //Assert.assertEquals(source.getMyProperty(JobProperties.WORK_UNIT_PARTITION), "weekly");
    Extract extract = source.createExtractObject(true);
    WorkUnit workUnit = WorkUnit.create(extract,
        new WatermarkInterval(new LongWatermark(1483257600000L), new LongWatermark(1572660000000L)));
    workUnit.setProp("ms.watermark.groups", "[\"watermark.datetime\",\"watermark.unit\"]");
    workUnit.setProp("watermark.datetime", "(1483257600000,1572660000000)");
    workUnit.setProp("watermark.unit", "NONE");
    WorkUnit workUnit1 = (WorkUnit) source.getWorkunits(state).get(0);
    Assert.assertEquals(workUnit1.getLowWatermark().toString(), workUnit.getLowWatermark().toString());
    Assert.assertEquals(workUnit1.getProp(ConfigurationKeys.DATASET_URN_KEY), "[watermark.system.1483257600000, watermark.unit.{}]");
    Assert.assertEquals(workUnit1.getProp(MultistageProperties.MSTAGE_WATERMARK_GROUPS.toString()), "[\"watermark.system\",\"watermark.unit\"]");
  }

  @Test
  public void testGetWorkUnitsWithSecondaryInput(){
    SourceState state = new SourceState();
    state.setProp("ms.watermark", "[{\"name\": \"system\",\"type\": \"datetime\", \"range\": {\"from\": \"2017-01-01\", \"to\": \"-\"}}]");
    state.setProp("extract.table.type", "SNAPSHOT_ONLY");
    state.setProp("extract.namespace", "test");
    state.setProp("extract.table.name", "table1");
    state.setProp("ms.work.unit.partition", "");
    state.setProp("ms.pagination", "{}");
    String secondaryInput = "[{\"path\": \"" +
        this.getClass().getResource("/util/avro").toString() + "\", \"fields\": [\"id\"]}]";
    state.setProp("ms.secondary.input", secondaryInput);


    MultistageSource source = new MultistageSource();
    Assert.assertNotNull(source);

    source.getWorkunits(state);

    Extract extract = source.createExtractObject(true);
    Assert.assertNotNull(extract);

    WorkUnit workUnit = WorkUnit.create(extract,
        new WatermarkInterval(new LongWatermark(1546329600000L), new LongWatermark(1572660000000L)));
    Assert.assertNotNull(workUnit);

    workUnit.setProp("ms.watermark.groups", "[\"watermark.datetime\",\"watermark.unit\"]");
    workUnit.setProp("watermark.datetime", "(1546329600000,1572660000000)");
    workUnit.setProp("watermark.unit", "NONE");

    List<WorkUnit> workUnits = (List<WorkUnit>) source.getWorkunits(state);
    Assert.assertEquals(workUnits.size(), 87);
    Assert.assertEquals(workUnits.get(0).getProp(ConfigurationKeys.DATASET_URN_KEY), "[watermark.system.1483257600000, watermark.activation.{\"id\":\"179513\"}]");
    Assert.assertEquals(workUnits.get(0).getProp(MultistageProperties.MSTAGE_ACTIVATION_PROPERTY.toString()).toString(), "{\"id\":\"179513\"}");
  }

  @Test
  public void testGetWorkUnitsWithSecondaryInputWithNullAuthentication() {
    SourceState state = new SourceState();
    state.setProp("ms.watermark", "[{\"name\": \"system\",\"type\": \"datetime\", \"range\": {\"from\": \"2017-01-01\", \"to\": \"-\"}}]");
    state.setProp("extract.table.type", "SNAPSHOT_ONLY");
    state.setProp("extract.namespace", "test");
    state.setProp("extract.table.name", "table1");
    state.setProp("ms.work.unit.partition", "");
    state.setProp("ms.pagination", "{}");
    String secondaryInput = "[{\"path\": \"" + this.getClass().getResource("/util/avro_retry").toString() + "\", "
        + "\"fields\": [], \"category\": \"authentication\"}]";
    state.setProp("ms.secondary.input", secondaryInput);


    MultistageSource source = new MultistageSource();
    Assert.assertNotNull(source);

    source.getWorkunits(state);
    List<WorkUnit> workUnits = (List<WorkUnit>) source.getWorkunits(state);

    Extract extract = source.createExtractObject(true);
    Assert.assertNotNull(extract);

    WorkUnit workUnit = WorkUnit.create(extract,
        new WatermarkInterval(new LongWatermark(1546329600000L), new LongWatermark(1572660000000L)));
    Assert.assertNotNull(workUnit);

    workUnit.setProp("ms.watermark.groups", "[\"watermark.datetime\",\"watermark.unit\"]");
    workUnit.setProp("watermark.datetime", "(1546329600000,1572660000000)");
    workUnit.setProp("watermark.unit", "NONE");


    Assert.assertEquals(workUnits.size(), 1);
  }

  @Test
  public void testGetWorkUnitsWithSecondaryInputWithAuthenticationRetriesDefined() {
    SourceState state = new SourceState();
    state.setProp("ms.watermark", "[{\"name\": \"system\",\"type\": \"datetime\", \"range\": {\"from\": \"2017-01-01\", \"to\": \"-\"}}]");
    state.setProp("extract.table.type", "SNAPSHOT_ONLY");
    state.setProp("extract.namespace", "test");
    state.setProp("extract.table.name", "table1");
    state.setProp("ms.work.unit.partition", "");
    state.setProp("ms.pagination", "{}");
    String secondaryInput = "[{\"path\": \"" + this.getClass().getResource("/util/avro_retry").toString() + "\", "
        + "\"fields\": [\"id\"], \"category\": \"authentication\", "
        + "\"retry\": {\"delayInSec\" : \"1\", \"retryCount\" : \"2\"}}]";
    state.setProp("ms.secondary.input", secondaryInput);


    MultistageSource source = new MultistageSource();
    Assert.assertNotNull(source);

    source.getWorkunits(state);
    List<WorkUnit> workUnits = (List<WorkUnit>) source.getWorkunits(state);

    Extract extract = source.createExtractObject(true);
    Assert.assertNotNull(extract);

    WorkUnit workUnit = WorkUnit.create(extract,
        new WatermarkInterval(new LongWatermark(1546329600000L), new LongWatermark(1572660000000L)));
    Assert.assertNotNull(workUnit);

    workUnit.setProp("ms.watermark.groups", "[\"watermark.datetime\",\"watermark.unit\"]");
    workUnit.setProp("watermark.datetime", "(1546329600000,1572660000000)");
    workUnit.setProp("watermark.unit", "NONE");


    Assert.assertEquals(workUnits.size(), 1);
  }

  @Test
  public void testGetWorkUnitsWithSecondaryInputWithAuthenticationRetriesNotDefined() {
    SourceState state = new SourceState();
    state.setProp("ms.watermark", "[{\"name\": \"system\",\"type\": \"datetime\", \"range\": {\"from\": \"2017-01-01\", \"to\": \"-\"}}]");
    state.setProp("extract.table.type", "SNAPSHOT_ONLY");
    state.setProp("extract.namespace", "test");
    state.setProp("extract.table.name", "table1");
    state.setProp("ms.work.unit.partition", "");
    state.setProp("ms.pagination", "{}");
    String secondaryInput = "[{\"path\": \"" + this.getClass().getResource("/util/avro_retry").toString() + "\", "
        + "\"fields\": [\"id\"], \"category\": \"authentication\"}]";
    state.setProp("ms.secondary.input", secondaryInput);


    MultistageSource source = new MultistageSource();
    Assert.assertNotNull(source);

    source.getWorkunits(state);
    List<WorkUnit> workUnits = (List<WorkUnit>) source.getWorkunits(state);

    Extract extract = source.createExtractObject(true);
    Assert.assertNotNull(extract);

    WorkUnit workUnit = WorkUnit.create(extract,
        new WatermarkInterval(new LongWatermark(1546329600000L), new LongWatermark(1572660000000L)));
    Assert.assertNotNull(workUnit);

    workUnit.setProp("ms.watermark.groups", "[\"watermark.datetime\",\"watermark.unit\"]");
    workUnit.setProp("watermark.datetime", "(1546329600000,1572660000000)");
    workUnit.setProp("watermark.unit", "NONE");


    Assert.assertEquals(workUnits.size(), 1);
  }


  @Test
  public void testParallismMaxSetting() {
    SourceState state = mock(SourceState.class);
    when(state.getPropAsInt("ms.work.unit.parallelism.max",0)).thenReturn(0);
    when(state.getProp("ms.pagination", new JsonObject().toString())).thenReturn("");

    Assert.assertFalse(MultistageProperties.MSTAGE_WORK_UNIT_PARALLELISM_MAX.validateNonblank(state));

    when(state.getPropAsInt("ms.work.unit.parallelism.max",0)).thenReturn(10);
    Assert.assertTrue(MultistageProperties.MSTAGE_WORK_UNIT_PARALLELISM_MAX.validateNonblank(state));
  }

  @Test
  public void testDerivedFields() {
    SourceState sourceState = mock(SourceState.class);
    when(sourceState.getProp("extract.table.type", "SNAPSHOT_ONLY")).thenReturn("SNAPSHOT_ONLY");
    when(sourceState.getProp("extract.namespace", "")).thenReturn("test");
    when(sourceState.getProp("extract.table.name", "")).thenReturn("table1");
    when(sourceState.getProp("ms.derived.fields", new JsonArray().toString())).thenReturn("[{\"name\": \"activityDate\", \"formula\": {\"type\": \"epoc\", \"source\": \"fromDateTime\", \"format\": \"yyyy-MM-dd'T'HH:mm:ss'Z'\"}}]");
    when(sourceState.getProp("ms.output.schema", new JsonArray().toString())).thenReturn("");
    when(sourceState.getProp(MultistageProperties.MSTAGE_OUTPUT_SCHEMA.getConfig(), "")).thenReturn("");
    MultistageSource source = new MultistageSource();
    source.getWorkunits(sourceState);

    Assert.assertEquals(source.getSourceKeys().getDerivedFields().keySet().toString(), "[activityDate]");
  }

  @Test
  public void testOutputSchema(){
    SourceState state = mock(SourceState.class);
    when(state.getProp("ms.output.schema", new JsonArray().toString())).thenReturn("");
    when(state.getProp(MultistageProperties.MSTAGE_OUTPUT_SCHEMA.getConfig(), "")).thenReturn("");
    when(state.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY, "SNAPSHOT_ONLY")).thenReturn("SNAPSHOT_ONLY");
    MultistageSource source = new MultistageSource();
    source.getWorkunits(state);
    Assert.assertEquals(0, source.getSourceKeys().getOutputSchema().getSchema().entrySet().size());

    // wrong format should be ignored
    when(state.getProp("ms.output.schema", new JsonArray().toString())).thenReturn("{\"name\": \"responseTime\"}");
    source.getWorkunits(state);
    Assert.assertEquals(0, source.getSourceKeys().getOutputSchema().getSchema().entrySet().size());

    // wrong format should be ignored
    when(state.getProp("ms.output.schema", new JsonArray().toString())).thenReturn("[{\"name\": \"responseTime\"}]");
    source.getWorkunits(state);
    Assert.assertEquals(1, source.getSourceKeys().getOutputSchema().getSchema().entrySet().size());
    Assert.assertEquals(1, source.getSourceKeys().getOutputSchema().getSchema().get("items").getAsJsonArray().size());
  }

  @Test
  public void testSourceParameters(){
    SourceState sourceState = mock(SourceState.class);
    when(sourceState.getProp(MultistageProperties.MSTAGE_OUTPUT_SCHEMA.getConfig(), "")).thenReturn("");
    when(sourceState.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY, "SNAPSHOT_ONLY")).thenReturn("SNAPSHOT_ONLY");
    MultistageSource source = new MultistageSource();
    source.getWorkunits(sourceState);
    Assert.assertNotNull(source.getSourceKeys().getSourceParameters());

    when(sourceState.getProp("ms.parameters", new JsonArray().toString())).thenReturn("[{\"name\":\"cursor\",\"type\":\"session\"}]");
    source.getWorkunits(sourceState);
    Assert.assertNotNull(source.getSourceKeys().getSourceParameters());
  }

  @Test
  public void testHadoopFsEncoding() {
    String plain = "[watermark.system.1483257600000, watermark.activation.{\"s3key\":\"cc-index/collections/CC-MAIN-2019-43/indexes/cdx-00000.gz\"}]";
    String expected = "[watermark.system.1483257600000, watermark.activation.{\"s3key\":\"cc-index%2Fcollections%2FCC-MAIN-2019-43%2Findexes%2Fcdx-00000.gz\"}]";
    String encoded = new MultistageSource().getHadoopFsEncoded(plain);
    Assert.assertEquals(encoded, expected);
  }

  @Test
  public void testUrlEncoding() {
    String plain = "{a b}";
    String expected = "%7Ba+b%7D";
    String encoded = new MultistageSource().getEncodedUtf8(plain);
    Assert.assertEquals(encoded, expected);
  }

  @Test
  public void testUnitWatermark(){
    SourceState state = new SourceState();
    state.setProp("ms.watermark", "[{\"name\": \"system\",\"type\": \"datetime\", \"range\": {\"from\": \"2020-01-01\", \"to\": \"2020-01-31\"}}, {\"name\": \"units\",\"type\": \"unit\", \"units\": \"id1,id2,id3\"}]");
    state.setProp("extract.table.type", "SNAPSHOT_ONLY");
    state.setProp("extract.namespace", "test");
    state.setProp("extract.table.name", "table1");
    state.setProp("ms.work.unit.partition", "");
    state.setProp("ms.pagination", "{}");
    MultistageSource source = new MultistageSource();
    Assert.assertEquals(source.getWorkunits(state).size(), 3);
  }

  @Test
  public void testIsSecondaryAuthenticationEnabled() {
    SourceState state = new SourceState();
    String secondaryInput = "[{\"path\": \"" + this.getClass().getResource("/util/avro_retry").toString() + "\", "
        + "\"fields\": [\"id\"], \"category\": \"authentication\", "
        + "\"retry\": {\"delayInSec\" : \"1\", \"retryCount\" : \"2\"}}]";
    state.setProp("ms.secondary.input", secondaryInput);
    MultistageSource source = new MultistageSource();
    source.initialize(state);
    Assert.assertTrue(source.isSecondaryAuthenticationEnabled());

    String secondaryInput2 = "[{\"path\": \"" + this.getClass().getResource("/util/avro_retry").toString() + "\", "
        + "\"fields\": [\"id\"], \"category\": \"activation\", "
        + "\"retry\": {\"delayInSec\" : \"1\", \"retryCount\" : \"2\"}}]";
    state.setProp("ms.secondary.input", secondaryInput2);
    source.initialize(state);
    Assert.assertFalse(source.isSecondaryAuthenticationEnabled());
  }

  @Test
  public void testIsSecondaryAuthenticationEnabledWithInvalidSecondaryInput() {
    SourceKeys sourceKeys = Mockito.mock(SourceKeys.class);
    source.sourceKeys = sourceKeys;
    JsonArray secondaryInput = gson.fromJson("[\"test_field\"]", JsonArray.class);
    when(sourceKeys.getSecondaryInputs()).thenReturn(secondaryInput);
    Assert.assertFalse(source.isSecondaryAuthenticationEnabled());
  }

  @Test
  public void testGetNext() {
    SourceKeys sourceKeys = Mockito.mock(SourceKeys.class);
    when(sourceKeys.getCallInterval()).thenReturn(1L);

    MultistageExtractor extractor = Mockito.mock(MultistageExtractor.class);
    ExtractorKeys extractorKeys = Mockito.mock(ExtractorKeys.class);
    WorkUnitStatus workUnitStatus = Mockito.mock(WorkUnitStatus.class);
    WorkUnitStatus.WorkUnitStatusBuilder builder = Mockito.mock(WorkUnitStatus.WorkUnitStatusBuilder.class);

    when(extractorKeys.getSignature()).thenReturn("testSignature");
    when(extractorKeys.getActivationParameters()).thenReturn(new JsonObject());
    when(builder.build()).thenReturn(workUnitStatus);
    when(extractor.getExtractorKeys()).thenReturn(extractorKeys);
    when(extractor.getWorkUnitStatus()).thenReturn(workUnitStatus);
    when(workUnitStatus.toBuilder()).thenReturn(builder);

    Assert.assertEquals(source.getNext(extractor), workUnitStatus);

    WorkUnitStatus unitStatus = Mockito.mock(WorkUnitStatus.class);
    source.memberParameters.put(extractor, new JsonObject());
    source.memberExtractorStatus.put(extractor, unitStatus);
    Assert.assertEquals(source.getNext(extractor), workUnitStatus);

    source.sourceKeys = sourceKeys;
    when(sourceKeys.getSourceParameters()).thenReturn(new JsonArray());
    when(sourceKeys.getCallInterval()).thenThrow(Mockito.mock(IllegalArgumentException.class));
    source.getNext(extractor);
    Assert.assertEquals(source.getNext(extractor), workUnitStatus);
  }

  @Test
  public void testReadSecondaryAuthentication() {
    JsonArray
        secondaryInput = gson.fromJson("[{\"fields\": [\"access_token\"], \"category\": \"authentication\"}]", JsonArray.class);
    SourceKeys sourceKeys = Mockito.mock(SourceKeys.class);
    State state = Mockito.mock(State.class);
    when(sourceKeys.getSecondaryInputs()).thenReturn(secondaryInput);
    source.sourceKeys = sourceKeys;
    Assert.assertEquals(source.readSecondaryAuthentication(state, 1L).toString(), "{}");
  }

  @Test
  public void testGetUpdatedWorkUnitActivation() {
    WorkUnit workUnit = Mockito.mock(WorkUnit.class);
    JsonObject authentication = gson.fromJson("{\"method\": \"basic\", \"encryption\": \"base64\", \"header\": \"Authorization\"}", JsonObject.class);
    when(workUnit.getProp(MultistageProperties.MSTAGE_ACTIVATION_PROPERTY.toString(), StringUtils.EMPTY)).thenReturn(StringUtils.EMPTY);
    Assert.assertEquals(source.getUpdatedWorkUnitActivation(workUnit, authentication), authentication.toString());
  }

  @Test
  public void testConvertListToInputStream() throws IOException {
    Assert.assertNull(source.convertListToInputStream(null));

    List<String> stringList = new ArrayList<>();
    Assert.assertNull(source.convertListToInputStream(stringList));

    stringList.add("testString");
    Assert.assertEquals(IOUtils.toString(source.convertListToInputStream(stringList), StandardCharsets.UTF_8),
        String.join("\n", stringList));
  }

  /**
   * Test getExtractor when exception is thrown
   */
  @Test(expectedExceptions = RuntimeException.class)
  public void testGetExtractorWithException() {
    WorkUnitState state = Mockito.mock(WorkUnitState.class);
    source.getExtractor(state);
  }

  /**
   * Test generateWorkUnits when there are more than one DATETIME datetime type watermarks
   * Expected: RuntimeException
   */
  @Test(expectedExceptions = RuntimeException.class)
  public void testGenerateWorkUnitsWithException1() {
    testGenerateWorkUnitsHelper(WatermarkDefinition.WatermarkTypes.DATETIME);
  }

  /**
   * Test generateWorkUnits when there are more than one UNIT type watermarks
   * Expected: RuntimeException
   */
  @Test(expectedExceptions = RuntimeException.class)
  public void testGenerateWorkUnitsWithException2() {
    testGenerateWorkUnitsHelper(WatermarkDefinition.WatermarkTypes.UNIT);
  }

  private void testGenerateWorkUnitsHelper(WatermarkDefinition.WatermarkTypes watermarkTypes) {
    SourceState sourceState = Mockito.mock(SourceState.class);
    source.sourceState = sourceState;

    WatermarkDefinition watermarkDefinition1 = Mockito.mock(WatermarkDefinition.class);
    WatermarkDefinition watermarkDefinition2 = Mockito.mock(WatermarkDefinition.class);
    when(watermarkDefinition1.getType()).thenReturn(watermarkTypes);
    when(watermarkDefinition2.getType()).thenReturn(watermarkTypes);
    List<WatermarkDefinition> definitions = ImmutableList.of(watermarkDefinition1, watermarkDefinition2);

    Map<String, Long> previousHighWatermarks = new HashMap<>();
    source.generateWorkUnits(definitions, previousHighWatermarks);
  }

  @Test
  public void testAppendActivationParameter() throws Exception {
    MultistageExtractor extractor = Mockito.mock(MultistageExtractor.class);
    ExtractorKeys extractorKeys = Mockito.mock(ExtractorKeys.class);
    when(extractor.getExtractorKeys()).thenReturn(extractorKeys);
    JsonObject obj = gson.fromJson("{\"survey\": \"id1\"}", JsonObject.class);
    when(extractorKeys.getActivationParameters()).thenReturn(obj);

    Method method = MultistageSource.class.getDeclaredMethod("appendActivationParameter", MultistageExtractor.class, JsonObject.class);
    method.setAccessible(true);
    Assert.assertEquals(method.invoke(source, extractor, obj), obj);
  }

  @Test
  public void testGetUpdatedWorkUnitVariableValues() throws Exception {
    MultistageExtractor extractor = Mockito.mock(MultistageExtractor.class);
    WorkUnitStatus wuStatus = Mockito.mock(WorkUnitStatus.class);

    when(extractor.getWorkUnitStatus()).thenReturn(wuStatus);
    when(wuStatus.getPageSize()).thenReturn(100L);
    when(wuStatus.getPageNumber()).thenReturn(5L);
    when(wuStatus.getPageStart()).thenReturn(1L);
    when(wuStatus.getSessionKey()).thenReturn("test_session_key");

    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty(ParameterTypes.SESSION.toString(), "{\"name\": \"status\"}");
    jsonObject.addProperty(ParameterTypes.PAGESTART.toString(), 1);
    jsonObject.addProperty(ParameterTypes.PAGESIZE.toString(), 100);
    jsonObject.addProperty(ParameterTypes.PAGENO.toString(), 5);

    Method method = MultistageSource.class.getDeclaredMethod("getUpdatedWorkUnitVariableValues", MultistageExtractor.class, JsonObject.class);
    method.setAccessible(true);

    Assert.assertEquals(method.invoke(source, extractor, jsonObject).toString(),
        "{\"session\":\"test_session_key\",\"pagestart\":1,\"pagesize\":100,\"pageno\":5}");

    when(wuStatus.getPageSize()).thenReturn(-1L);
    Assert.assertEquals(method.invoke(source, extractor, jsonObject).toString(),
        "{\"pagesize\":100,\"session\":\"test_session_key\",\"pagestart\":1,\"pageno\":5}");
  }

  @Test
  public void testGetInitialWorkUnitVariableValues() throws Exception {
    MultistageExtractor extractor = Mockito.mock(MultistageExtractor.class);
    Method method = MultistageSource.class.getDeclaredMethod("getInitialWorkUnitVariableValues", MultistageExtractor.class);
    method.setAccessible(true);

    SourceKeys sourceKeys = Mockito.mock(SourceKeys.class);
    source.sourceKeys = sourceKeys;
    JsonObject waterMarkObj = gson.fromJson("{\"watermark\":{\"low\":-100,\"high\":1564642800}}", JsonObject.class);
    when(extractor.getWorkUnitWaterMarks()).thenReturn(waterMarkObj);
    when(sourceKeys.getPaginationInitValues()).thenReturn(ImmutableMap.of(ParameterTypes.PAGESIZE, 10L));
    Assert.assertEquals(method.invoke(source, extractor).toString(),
        "{\"watermark\":{\"watermark\":{\"low\":-100,\"high\":1564642800}},\"pagesize\":10}");
  }

  @Test
  public void testGetDefaultFieldTypes() throws Exception {
    Method method = MultistageSource.class.getDeclaredMethod("getDefaultFieldTypes", State.class);
    method.setAccessible(true);

    State state = Mockito.mock(State.class);
    when(state.getProp(MultistageProperties.MSTAGE_DATA_DEFAULT_TYPE.getConfig(), new JsonObject().toString())).thenReturn("{\"testField\":100}");
    Assert.assertEquals(method.invoke(source, state).toString(), "{testField=100}");
  }

  @Test
  public void testGetPaginationInitialValues() throws Exception {

    Method method = MultistageSource.class.getDeclaredMethod("getPaginationInitialValues", State.class);
    method.setAccessible(true);

    State state = Mockito.mock(State.class);
    when(state.getProp(MultistageProperties.MSTAGE_PAGINATION.getConfig(), new JsonObject().toString()))
        .thenReturn("{\"fields\": [\"offset\", \"limit\"], \"initialvalues\": [0, 5000]}");
    method.invoke(source, state);
    Map<ParameterTypes, Long> paginationInitValues = source.getSourceKeys().getPaginationInitValues();
    Assert.assertEquals((long) paginationInitValues.get(ParameterTypes.PAGESTART), 0L);
    Assert.assertEquals((long) paginationInitValues.get(ParameterTypes.PAGESIZE), 5000L);
  }

  @Test
  public void testGetPaginationFields() throws Exception {
    State state = Mockito.mock(State.class);
    when(state.getProp(MultistageProperties.MSTAGE_PAGINATION.getConfig(), new JsonObject().toString()))
        .thenReturn("{\"fields\": [\"\", \"\"], \"initialvalues\": [0, 5000]}");
    Method method = MultistageSource.class.getDeclaredMethod("getPaginationFields", State.class);
    method.setAccessible(true);
    method.invoke(source, state);
    Assert.assertEquals(source.getSourceKeys().getPaginationInitValues().size(), 0);

    when(state.getProp(MultistageProperties.MSTAGE_PAGINATION.getConfig(), new JsonObject().toString()))
        .thenReturn("{\"initialvalues\": [0, 5000]}");
    method.invoke(source, state);
    Assert.assertEquals(source.getSourceKeys().getPaginationInitValues().size(), 0);
  }

  @Test
  public void testGetPreviousHighWatermarks() throws Exception {
    SourceState sourceState = Mockito.mock(SourceState.class);
    WorkUnitState workUnitState = Mockito.mock(WorkUnitState.class);
    source.sourceState = sourceState;

    Map<String, Iterable<WorkUnitState>> previousWorkUnitStatesByDatasetUrns = new HashMap<>();
    previousWorkUnitStatesByDatasetUrns.put("ColumnName.Number", ImmutableList.of(workUnitState));
    when(workUnitState.getActualHighWatermark(LongWatermark.class)).thenReturn(new LongWatermark(1000L));
    when(sourceState.getPreviousWorkUnitStatesByDatasetUrns()).thenReturn(previousWorkUnitStatesByDatasetUrns);

    Method method = MultistageSource.class.getDeclaredMethod("getPreviousHighWatermarks");
    method.setAccessible(true);
    Map<String, Long> actual = (Map) method.invoke(source);
    Assert.assertEquals(actual.size(), 1);
    Assert.assertEquals((long) actual.get("ColumnName.Number"), 1000L);
  }

  @Test
  public void testParseSecondaryInputRetry() throws Exception {
    JsonArray input = gson.fromJson("[{\"retry\": {\"threadpool\": 5}}]", JsonArray.class);
    Method method = MultistageSource.class.getDeclaredMethod("parseSecondaryInputRetry", JsonArray.class);
    method.setAccessible(true);
    Map<String, Long> actual = (Map) method.invoke(source, input);
    Assert.assertEquals((long) actual.get("delayInSec"), 300L);
    Assert.assertEquals((long) actual.get("retryCount"), 3);

    input = gson.fromJson("[{\"retry\": {\"delayInSec\": 500,\"retryCount\": 5}}]", JsonArray.class);
    actual = (Map) method.invoke(source, input);
    Assert.assertEquals((long) actual.get("delayInSec"), 500L);
    Assert.assertEquals((long) actual.get("retryCount"), 5);
  }

  @Test
  public void testReadSecondaryInputs() throws Exception {
    SourceKeys sourceKeys = Mockito.mock(SourceKeys.class);
    State state = new State();
    long retries = 1L;
    String avroFilePath = this.getClass().getResource("/util/avro/surveys.avro").getPath();
    String inputWithoutPath = "[{\"path\": \"%s\", \"fields\": [\"active\",\"id\",\"name\"], \"filters\": {\"active\": \"True\"}}]";
    JsonArray secondaryInput = gson.fromJson(String.format(inputWithoutPath, avroFilePath), JsonArray.class);

    source.sourceKeys = sourceKeys;
    when(sourceKeys.getSecondaryInputs()).thenReturn(secondaryInput);
    Method method = MultistageSource.class.getDeclaredMethod("readSecondaryInputs", State.class, long.class);
    method.setAccessible(true);
    Map actual = (Map) method.invoke(source, state, retries);
    Assert.assertEquals(actual.get("activation").toString(), "[]");

    avroFilePath = this.getClass().getResource("/avro-access-token/accessToken.avro").getPath();
    inputWithoutPath = "[{\"path\": \"%s\", \"fields\": [\"access_token\"], \"category\": \"authentication\", \"retry\": {\"delayInSec\" : \"3\", \"retryCount\" : \"2\"}}]";
    secondaryInput = gson.fromJson(String.format(inputWithoutPath, avroFilePath), JsonArray.class);
    when(sourceKeys.getSecondaryInputs()).thenReturn(secondaryInput);
    actual = (Map) method.invoke(source, state, retries);
    Assert.assertEquals(actual.get("authentication").toString(),
        "[{\"access_token\":\"00D300000001Xri!ARwAQAnxxlcm3_4FX9.Qp39Av5uN_DzrHMKC9Ehp39HDXIRtDvKKNpYWNjlIjPqsZb0GTlWgIhO5Aj6CPyOT3aGEc9a1gTjC\"},{\"access_token\":\"00D300000001Xri!ARwAQAnxxlcm3_4FX9.Qp39Av5uN_DzrHMKC9Ehp39HDXIRtDvKKNpYWNjlIjPqsZb0GTlWgIhO5Aj6CPyOT3aGEc9a1gTjC\"}]");
  }

  /**
   * Test normal cases
   */
  @Test
  public void testGetWorkUnitPartitionTypes() {
    SourceState state = new SourceState();
    source = new MultistageSource();

    state.setProp("ms.work.unit.partition", "");
    source.initialize(state);
    Assert.assertEquals(source.getPartitionType(state), WorkUnitPartitionTypes.NONE);

    state.setProp("ms.work.unit.partition", "none");
    source.initialize(state);
    Assert.assertEquals(source.getPartitionType(state), WorkUnitPartitionTypes.NONE);

    state.setProp("ms.work.unit.partition", "weekly");
    source.initialize(state);
    Assert.assertEquals(source.getPartitionType(state), WorkUnitPartitionTypes.WEEKLY);

    state.setProp("ms.work.unit.partition", "monthly");
    source.initialize(state);
    Assert.assertEquals(source.getPartitionType(state), WorkUnitPartitionTypes.MONTHLY);

    state.setProp("ms.work.unit.partition", "daily");
    source.initialize(state);
    Assert.assertEquals(source.getPartitionType(state), WorkUnitPartitionTypes.DAILY);

    state.setProp("ms.work.unit.partition", "hourly");
    source.initialize(state);
    Assert.assertEquals(source.getPartitionType(state), WorkUnitPartitionTypes.HOURLY);

    state.setProp("ms.work.unit.partition", "{\"none\": [\"2020-01-01\", \"2020-02-18\"]}");
    state.setProp("ms.work.unit.partial.partition", false);
    source.initialize(state);
    Assert.assertEquals(source.getPartitionType(state), WorkUnitPartitionTypes.COMPOSITE);
    Assert.assertEquals(source.getPartitionType(state).getRanges(
        DateTime.parse("2020-01-01"),
        DateTime.parse("2020-02-18"),
        source.sourceKeys.getIsPartialPartition()).size(), 1);

    state.setProp("ms.work.unit.partition", "{\"monthly\": [\"2020-01-01\", \"2020-02-18\"]}");
    state.setProp("ms.work.unit.partial.partition", false);
    source.initialize(state);
    Assert.assertEquals(source.getPartitionType(state), WorkUnitPartitionTypes.COMPOSITE);
    Assert.assertEquals(source.getPartitionType(state).getRanges(
        DateTime.parse("2020-01-01"),
        DateTime.parse("2020-02-18"),
        source.sourceKeys.getIsPartialPartition()).size(), 1);

    state.setProp("ms.work.unit.partition", "{\"monthly\": [\"2020-01-01\", \"2020-02-18\"]}");
    state.setProp("ms.work.unit.partial.partition", true);
    source.initialize(state);
    Assert.assertEquals(source.getPartitionType(state), WorkUnitPartitionTypes.COMPOSITE);
    Assert.assertEquals(source.getPartitionType(state).getRanges(
        DateTime.parse("2020-01-01"),
        DateTime.parse("2020-02-18"),
        source.sourceKeys.getIsPartialPartition()).size(), 2);

    state.setProp("ms.work.unit.partition", "{\"weekly\": [\"2020-01-01\", \"2020-02-01\"]}");
    state.setProp("ms.work.unit.partial.partition", true);
    source.initialize(state);
    Assert.assertEquals(source.getPartitionType(state), WorkUnitPartitionTypes.COMPOSITE);
    Assert.assertEquals(source.getPartitionType(state).getRanges(
        DateTime.parse("2020-01-01"),
        DateTime.parse("2020-02-01"),
        source.sourceKeys.getIsPartialPartition()).size(), 5);

    // this should gives out 3 ranges: 1/1 - 2/1, 2/1 - 2/2, 2/2 - 2/3
    state.setProp("ms.work.unit.partition", "{\"monthly\": [\"2020-01-01T00:00:00-00:00\", \"2020-02-01T00:00:00-00:00\"], \"daily\": [\"2020-02-01T00:00:00-00:00\", \"2020-02-03T00:00:00-00:00\"]}");
    state.setProp("ms.work.unit.partial.partition", false);
    source.initialize(state);
    Assert.assertEquals(source.getPartitionType(state), WorkUnitPartitionTypes.COMPOSITE);
    Assert.assertEquals(source.getPartitionType(state).getRanges(
        DateTime.parse("2020-01-01T00:00:00-00:00").withZone(DateTimeZone.UTC),
        DateTime.parse("2020-02-03T00:00:00-00:00").withZone(DateTimeZone.UTC),
        source.sourceKeys.getIsPartialPartition()).size(), 3);


    // this should gives out 3 ranges: 1/1 - 2/1, 2/1 - 2/2, 2/2 - 2/3
    state.setProp("ms.work.unit.partition", "{\"none\": [\"2010-01-01T00:00:00-00:00\", \"2020-02-01T00:00:00-00:00\"], \"daily\": [\"2020-02-01T00:00:00-00:00\", \"2020-02-03T00:00:00-00:00\"]}");
    state.setProp("ms.work.unit.partial.partition", false);
    source.initialize(state);
    Assert.assertEquals(source.getPartitionType(state), WorkUnitPartitionTypes.COMPOSITE);
    Assert.assertEquals(source.getPartitionType(state).getRanges(
        DateTime.parse("2010-01-01T00:00:00-00:00").withZone(DateTimeZone.UTC),
        DateTime.parse("2020-02-03T00:00:00-00:00").withZone(DateTimeZone.UTC),
        source.sourceKeys.getIsPartialPartition()).size(), 3);

    state.setProp("ms.work.unit.partition", "{\"monthly\": [\"2020-01-01\", \"-\"]}");
    state.setProp("ms.work.unit.partial.partition", true);
    source.initialize(state);
    Assert.assertEquals(source.getPartitionType(state), WorkUnitPartitionTypes.COMPOSITE);
    Assert.assertEquals(source.getPartitionType(state).getRanges(
        DateTime.now().monthOfYear().roundFloorCopy(),
        DateTime.now().monthOfYear().roundCeilingCopy(),
        source.sourceKeys.getIsPartialPartition()).size(), 1);
  }

  /**
   * test incorrect Json format
   */
  @Test
  public void testGetWorkUnitPartitionTypesWithExceptions1() {
    SourceState state = new SourceState();
    MultistageSource source = new MultistageSource();

    state.setProp("ms.work.unit.partition", "{\"monthly\": \"2020-01-01\"]}");
    state.setProp("ms.work.unit.partial.partition", true);
    source.initialize(state);
    Assert.assertEquals(source.getPartitionType(state), null);
  }

  /**
   * test nonconforming property format
   */
  @Test
  public void testGetWorkUnitPartitionTypesWithExceptions2() {
    SourceState state = new SourceState();
    MultistageSource source = new MultistageSource();

    // in this case, the partition range is ignored, and there is no partitioning
    state.setProp("ms.work.unit.partition", "{\"monthly\": [\"2020-01-01\"]}");
    state.setProp("ms.work.unit.partial.partition", false);
    source.initialize(state);
    Assert.assertEquals(source.getPartitionType(state), WorkUnitPartitionTypes.COMPOSITE);
    Assert.assertEquals(source.getPartitionType(state).getRanges(
        DateTime.now().monthOfYear().roundFloorCopy(),
        DateTime.now().monthOfYear().roundCeilingCopy(),
        source.sourceKeys.getIsPartialPartition()).size(), 0);

    // supposedly we wanted 5 weekly partitions, but the range end date time format is incorrect
    // therefore it will not generate the number of partitions as wanted
    state.setProp("ms.work.unit.partition", "{\"weekly\": [\"2020-01-01\", \"2020-02-1\"]}");
    state.setProp("ms.work.unit.partial.partition", true);
    source.initialize(state);
    Assert.assertEquals(source.getPartitionType(state), WorkUnitPartitionTypes.COMPOSITE);
    Assert.assertNotEquals(source.getPartitionType(state).getRanges(
        DateTime.parse("2001-01-01"),
        DateTime.parse("2090-01-01"),
        source.sourceKeys.getIsPartialPartition()).size(), 5);
  }

}

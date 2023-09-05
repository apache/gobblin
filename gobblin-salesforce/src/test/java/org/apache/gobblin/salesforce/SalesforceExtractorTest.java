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
package org.apache.gobblin.salesforce;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.exception.HighWatermarkException;
import org.apache.gobblin.source.extractor.exception.RestApiClientException;
import org.apache.gobblin.source.extractor.extract.Command;
import org.apache.gobblin.source.extractor.extract.restapi.RestApiCommand;
import org.apache.gobblin.source.extractor.partition.Partition;
import org.apache.gobblin.source.extractor.watermark.Predicate;
import org.apache.gobblin.source.extractor.watermark.TimestampWatermark;
import org.apache.gobblin.source.extractor.watermark.WatermarkType;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class SalesforceExtractorTest {

  private static final String DEFAULT_SCHEMA = "test-schema";
  private static final String DEFAULT_ENTITY = "test-entity";
  private static final String DEFAULT_WATERMARK_COLUMN = "test-watermark-column";
  private static final String GTE_OPERATOR = ">=";
  private static final String LTE_OPERATOR = "<=";
  private static final long LWM_VALUE_1 = 20131212121212L;
  private static final long HWM_VALUE_1 = 20231212121212L;
  private static final String DEFAULT_WATERMARK_VALUE_FORMAT = "yyyyMMddHHmmss";

  private SalesforceExtractor _classUnderTest;

  @BeforeTest
  public void beforeTest() {
    WorkUnit workUnit = WorkUnit.createEmpty();
    workUnit.setProp(Partition.IS_LAST_PARTIITON, false);
    workUnit.setProp(ConfigurationKeys.SOURCE_QUERYBASED_EXTRACT_TYPE, "SNAPSHOT");
    WorkUnitState workUnitState = new WorkUnitState(workUnit, new State());
    workUnitState.setId("test");
    _classUnderTest = new SalesforceExtractor(workUnitState);
  }

  @DataProvider
  private Object[][] provideGetHighWatermarkMetadataTestData() {

    TimestampWatermark watermark =
        new TimestampWatermark(DEFAULT_WATERMARK_COLUMN, DEFAULT_WATERMARK_VALUE_FORMAT);
    String lwmCondition = watermark.getWatermarkCondition(_classUnderTest, LWM_VALUE_1, GTE_OPERATOR);
    String hwmCondition = watermark.getWatermarkCondition(_classUnderTest, HWM_VALUE_1, LTE_OPERATOR);
    Predicate lwmPredicate =
        new Predicate(DEFAULT_WATERMARK_COLUMN, LWM_VALUE_1, lwmCondition,
            _classUnderTest.getWatermarkSourceFormat(WatermarkType.TIMESTAMP), Predicate.PredicateType.LWM);
    Predicate hwmPredicate =
        new Predicate(DEFAULT_WATERMARK_COLUMN, HWM_VALUE_1, hwmCondition,
            _classUnderTest.getWatermarkSourceFormat(WatermarkType.TIMESTAMP), Predicate.PredicateType.HWM);

    return new Object[][] {
        {
            // With low and high watermark predicates
            ImmutableList.of(lwmPredicate, hwmPredicate),
            String.format("SELECT MAX(%s) FROM %s where (%s) and (%s)",
                DEFAULT_WATERMARK_COLUMN, DEFAULT_ENTITY, lwmPredicate.getCondition(), hwmPredicate.getCondition())
        },
        {
            // With no predicates
            ImmutableList.of(),
            String.format("SELECT MAX(%s) FROM %s",
                DEFAULT_WATERMARK_COLUMN, DEFAULT_ENTITY)
        }
    };
  }

  @Test(dataProvider = "provideGetHighWatermarkMetadataTestData")
  public void testGetHighWatermarkMetadata(List<Predicate> predicateList,
      String restQueryExpected) throws HighWatermarkException, RestApiClientException {

    List<Command> commandsActual =
        _classUnderTest.getHighWatermarkMetadata(DEFAULT_SCHEMA, DEFAULT_ENTITY, DEFAULT_WATERMARK_COLUMN,
            predicateList);

    String fullUri = new SalesforceConnector(new State()).getFullUri(SalesforceExtractor.getSoqlUrl(restQueryExpected));
    List<Command> commandsExpected = Collections.singletonList(
        new RestApiCommand().build(Collections.singletonList(fullUri), RestApiCommand.RestApiCommandType.GET));

    Assert.assertEquals(commandsActual.size(), 1);
    Assert.assertEquals(commandsActual.get(0).getCommandType(), commandsExpected.get(0).getCommandType());
    Assert.assertEquals(commandsActual.get(0).getParams(), commandsExpected.get(0).getParams());
  }
}
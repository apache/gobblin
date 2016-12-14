/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.compliance;

import java.io.IOException;

import org.apache.hadoop.hive.ql.metadata.Partition;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import gobblin.configuration.WorkUnitState;
import gobblin.source.workunit.WorkUnit;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


public class HivePurgerExtractorTest {
  private WorkUnitState stateMock = Mockito.mock(WorkUnitState.class);
  private WorkUnit workUnitMock = Mockito.mock(WorkUnit.class);
  private HivePurgerPartitionRecord recordMock = Mockito.mock(HivePurgerPartitionRecord.class);
  private Partition partitionMock = Mockito.mock(Partition.class);
  private HivePurgerExtractor extractor;

  @BeforeTest
  public void initialize() {
    when(this.stateMock.getWorkunit()).thenReturn(this.workUnitMock);
    when(this.workUnitMock.getProp(anyString())).thenReturn("testString");
    this.extractor = new HivePurgerExtractor(this.stateMock);
    this.extractor.setRecord(this.recordMock);
  }

  @Test
  public void getSchemaTest() {
    Assert.assertNotNull(this.extractor.getSchema());
  }

  @Test
  public void readRecordTest()
      throws IOException {
    Assert.assertNotNull(this.extractor.readRecord(null));
    Assert.assertNull(this.extractor.readRecord(null));
    Assert.assertEquals(1, this.extractor.getExpectedRecordCount());
  }
}
